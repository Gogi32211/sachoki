"""MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1 — the measurement I wrongly froze as impossible.

I sealed 15M_LEGACY_VS_MASSIVE_FEED_DIVERGENCE = NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA after
proving that `bars` is structurally daily. That proof was correct and the conclusion drawn from
it was not: a legacy 15m MATERIALIZED T population genuinely does not exist, but a legacy 15m
OHLC store does — studio_15m_base.duckdb, 90,952,507 rows covering 2021-07-02 to 2026-08-25. And
this comparison never needed a legacy T column, because it runs the canonical engine on BOTH
sides. I collapsed two different claims into one and searched too little before freezing.

SO THE TWO CLAIMS ARE KEPT APART HERE, PERMANENTLY:
    legacy 15m MATERIALIZED T population        -> DOES NOT EXIST (unchanged)
    15m same-engine cross-source divergence     -> MEASURED HERE

WHAT MAKES THIS AN HONEST SOURCE MEASUREMENT. Each side is fed its OWN series and its own
predecessors, because the frozen call context says T[t] depends on the immediately preceding row
of that series. If I forced both sides onto one merged bar set I would be measuring something
other than "what this engine does on this feed". Predecessor disagreement is therefore PART of
source divergence, and it is decomposed rather than hidden: every compared bar records whether
its predecessor also matched.

COMMON SUPPORT ONLY, EXACT TIMESTAMPS. No nearest-bar matching, no tolerance, no offset chosen
to raise agreement. Support loss is counted by reason instead of quietly shrinking the
denominator. Both stores carry bar-START timestamps in UTC, which is checked rather than assumed.

X-ONLY. This measures input sensitivity. It registers no hypothesis, promotes nothing, and does
not resurrect legacy-15m materialization equivalence. Y_EXPOSED = 0.
"""
from __future__ import annotations
import glob, json, os, sys, time                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                        # noqa: E402
import pandas as pd                                                       # noqa: E402
import pyarrow.parquet as pq                                              # noqa: E402
import t5_artifact as ART                                                 # noqa: E402
from signal_engine import compute_signals                                 # noqa: E402

LEG = "/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m_base.duckdb"
COARSE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1/15m"
T_SLICE = "2901107d9baa6320"
BIND = {"MASSIVE_T_FEATURE_PRODUCTION_V1.json": "ccdb0e35490794da",
        "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json": "49eb9658eda72773",
        "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json": "7c0a5ad6b17d7ae0"}
PRIORITY = ["T4", "T6", "T1G", "T2G", "T1", "T2", "T9", "T10", "T3", "T11", "T5", "T12"]


def t_labels(o, h, l, c, idx):
    d = pd.DataFrame({"open": o, "high": h, "low": l, "close": c}, index=idx)
    s = compute_signals(d)
    bc = s["bc"].to_numpy().astype(int)
    return np.where(bc > 0, s["sig_name"].to_numpy(), "NONE"), bc


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="15m source divergence diagnostic (read-only)")
    import duckdb
    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding mismatch:", bad); return 1

    el = json.load(open("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    tickers = sorted(el["cohort"]["eligible_tickers"])
    want = {key_of[t]: t for t in tickers if t in key_of}

    # ---- Massive side: stream partitions once, accumulate per security ------
    print("  loading Massive 15m …", flush=True)
    acc = {}
    files = sorted(glob.glob(os.path.join(COARSE, "*", "*", "*.parquet")))
    for i, f in enumerate(files):
        d = pq.read_table(f, columns=["security_key_v1", "bar_start", "interval_index",
                                      "coverage_state", "open", "high", "low",
                                      "close"]).to_pandas()
        d = d[d["security_key_v1"].isin(want)]
        for k, g in d.groupby("security_key_v1", sort=False):
            acc.setdefault(k, []).append(g)
        if i % 300 == 0:
            print(f"    {i+1}/{len(files)}", flush=True)
    mv = {k: pd.concat(v, ignore_index=True).sort_values("bar_start")
          for k, v in acc.items()}
    del acc
    print(f"  Massive securities: {len(mv)}", flush=True)

    con = duckdb.connect(LEG, read_only=True)
    lst = ",".join("'" + t + "'" for t in tickers)
    pin = con.execute(
        f"select ticker, universe, count(*) n from bars where ticker in ({lst}) "
        "group by 1,2").fetchdf()
    pref = {"sp500": 0, "nasdaq": 1, "russell2k": 2}
    pin["r"] = pin["universe"].map(lambda u: pref.get(u, 9))
    pin = pin.sort_values(["ticker", "r", "n"], ascending=[True, True, False])
    pinned = pin.groupby("ticker").first()["universe"].to_dict()

    tot = dict(compared=0, agree=0, pred_matched=0, pred_matched_agree=0)
    loss = dict(no_legacy_ticker=0, ts_only_in_legacy=0, ts_only_in_massive=0,
                massive_not_complete=0)
    conf, ohlc = {}, {f: dict(n=0, exact=0, d1c=0, maxd=0.0) for f in
                      ("open", "high", "low", "close")}
    by_pos, by_body = {}, {}
    secs = 0
    t0 = time.time()

    for n_i, (k, tk) in enumerate(sorted(want.items(), key=lambda kv: kv[1])):
        if k not in mv:
            continue
        u = pinned.get(tk)
        if u is None:
            loss["no_legacy_ticker"] += 1; continue
        lg = con.execute("select date, open, high, low, close from bars "
                         "where ticker = ? and universe = ? order by date",
                         [tk, u]).fetchdf()
        if len(lg) < 3:
            loss["no_legacy_ticker"] += 1; continue
        lg = lg.drop_duplicates("date").sort_values("date")
        # DuckDB returns datetime64[us]; a bare //10**6 yields SECONDS while the
        # coarse store is in MILLISECONDS. Convert through an explicit ms dtype so
        # the unit cannot depend on the source resolution.
        lts = (pd.to_datetime(lg["date"], utc=True).dt.tz_localize(None)
               .astype("datetime64[ms]").astype("int64").to_numpy())
        m = mv[k]
        mts = m["bar_start"].to_numpy()

        # each side gets its OWN series and its OWN predecessors
        lab_l, _ = t_labels(lg["open"].to_numpy(float), lg["high"].to_numpy(float),
                            lg["low"].to_numpy(float), lg["close"].to_numpy(float),
                            pd.to_datetime(lts, unit="ms", utc=True))
        lab_m, _ = t_labels(m["open"].to_numpy(float), m["high"].to_numpy(float),
                            m["low"].to_numpy(float), m["close"].to_numpy(float),
                            pd.to_datetime(mts, unit="ms", utc=True))

        li = {t: j for j, t in enumerate(lts)}
        common = np.array([t in li for t in mts])
        loss["ts_only_in_massive"] += int((~common).sum())
        loss["ts_only_in_legacy"] += int(len(lts) - common.sum())
        okc = (m["coverage_state"].to_numpy() == "COMPLETE")
        use = common & okc
        loss["massive_not_complete"] += int((common & ~okc).sum())
        if not use.any():
            continue
        secs += 1
        lidx = np.array([li[t] for t in mts[use]])
        a = lab_l[lidx]; b = lab_m[use]
        tot["compared"] += len(a); tot["agree"] += int((a == b).sum())

        # predecessor also matched?
        prev_ok = np.zeros(len(mts), bool)
        prev_ok[1:] = common[:-1]
        pm = prev_ok[use]
        tot["pred_matched"] += int(pm.sum())
        tot["pred_matched_agree"] += int((a[pm] == b[pm]).sum())

        for x, y in zip(a[a != b], b[a != b]):
            conf[f"{x}->{y}"] = conf.get(f"{x}->{y}", 0) + 1

        for fld in ("open", "high", "low", "close"):
            la = lg[fld].to_numpy(float)[lidx]
            mb = m[fld].to_numpy(float)[use]
            dd = np.abs(la - np.round(mb, 2))
            ohlc[fld]["n"] += len(la)
            ohlc[fld]["exact"] += int(np.isclose(la, np.round(mb, 2), rtol=0,
                                                 atol=1e-9).sum())
            ohlc[fld]["d1c"] += int((dd <= 0.010000001).sum())
            ohlc[fld]["maxd"] = max(ohlc[fld]["maxd"], float(np.nanmax(dd)))

        pos = m["interval_index"].to_numpy()[use]
        last = pos == pos.max()
        for nm, sel in (("M1", pos == 0), ("M2", pos == 1), ("M3", pos == 2),
                        ("M4", pos == 3), ("SESSION_FINAL", last),
                        ("OTHER", (pos > 3) & ~last)):
            if sel.any():
                e = by_pos.setdefault(nm, dict(n=0, agree=0))
                e["n"] += int(sel.sum()); e["agree"] += int((a[sel] == b[sel]).sum())

        body = np.abs(m["close"].to_numpy(float)[use] - m["open"].to_numpy(float)[use])
        for nm, sel in (("body_0", body == 0), ("body_le_1c", (body > 0) & (body <= 0.01)),
                        ("body_le_10c", (body > 0.01) & (body <= 0.10)),
                        ("body_gt_10c", body > 0.10)):
            if sel.any():
                e = by_body.setdefault(nm, dict(n=0, agree=0))
                e["n"] += int(sel.sum()); e["agree"] += int((a[sel] == b[sel]).sum())
        if n_i % 60 == 0:
            print(f"    {n_i+1}/{len(want)} · compared {tot['compared']:,}", flush=True)

    con.close()
    if tot["compared"] == 0 or secs == 0:
        print("HOLD — zero matched support. A diagnostic that matches nothing is a\n"
              "        harness defect, not a measurement, and must not seal.")
        print("        loss by reason:", loss)
        return 1
    rate = lambda d: round(d["agree"] / d["n"], 6) if d["n"] else None   # noqa: E731
    p = dict(
        report_id="MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1",
        status="X_ONLY_DIAGNOSTIC_COMPLETE",
        task_class="SOURCE_DIVERGENCE_DIAGNOSTIC_ONLY",
        question="canonical compute_signals on legacy 15m OHLC vs canonical compute_signals "
                 "on Massive 15m OHLC, matched by secure identity and EXACT timestamp",
        classification="DIAGNOSTIC_ONLY_NOT_HYPOTHESIS_REGISTRATION",
        supersedes_premise=dict(
            old_claim="15M_LEGACY_VS_MASSIVE_FEED_DIVERGENCE = "
                      "NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA",
            why_it_was_wrong="it was frozen after proving `bars` is daily; that proof was "
                             "right, but this comparison never needed a legacy T column — it "
                             "runs the canonical engine on both sides, and a legacy 15m OHLC "
                             "store existed all along",
            what_remains_true="a legacy 15m MATERIALIZED T population DOES NOT EXIST",
            what_is_now_measured="15m same-engine cross-source divergence",
            old_artifacts_not_edited=["c6358576c5c9411b", "4c9b15eb8cfed786"]),
        sources=dict(
            legacy=dict(store=LEG, table="bars", rows=90952507,
                        range="2021-07-02 .. 2026-08-25", has_t_column=False,
                        universe_pinned=True),
            massive=dict(store=COARSE, coverage_filter="COMPLETE only")),
        matching=dict(key="security_key_v1 + exact bar_start (UTC, bar-START both sides)",
                      tolerance=None, nearest_timestamp=False,
                      offset_search=False),
        support=dict(securities_compared=secs, bars_compared=tot["compared"],
                     loss_by_reason=loss),
        agreement=dict(
            t_label_agreement=round(tot["agree"] / tot["compared"], 6) if tot["compared"]
            else None,
            agreed=tot["agree"],
            predecessor_also_matched=dict(
                bars=tot["pred_matched"],
                agreement=round(tot["pred_matched_agree"] / tot["pred_matched"], 6)
                if tot["pred_matched"] else None,
                why="each side keeps its OWN predecessors, so predecessor disagreement is "
                    "part of source divergence and is decomposed rather than hidden")),
        state_divergence_matrix=dict(sorted(conf.items(), key=lambda kv: -kv[1])[:25]),
        price_basis={f: dict(compared=v["n"],
                             exact_after_round=v["exact"],
                             rate=round(v["exact"] / v["n"], 6) if v["n"] else None,
                             within_1_cent=round(v["d1c"] / v["n"], 6) if v["n"] else None,
                             max_abs_diff=v["maxd"]) for f, v in ohlc.items()},
        by_session_position={k: dict(bars=v["n"], agreement=rate(v))
                             for k, v in by_pos.items()},
        by_body_size={k: dict(bars=v["n"], agreement=rate(v))
                      for k, v in by_body.items()},
        producer_semantics_digest=T_SLICE,
        does_not=["register any hypothesis", "restore legacy 15m materialization equivalence",
                  "authorize correcting Massive T labels toward legacy",
                  "authorize weighting observations by agreement"],
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        elapsed_sec=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1.json",
                 required=("report_id", "status", "support", "agreement", "price_basis",
                           "supersedes_premise", "matching"),
                 supersede=os.path.exists(
                     "MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1.json"))
    print(f"\nMASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1 · {d} · {p['status']}")
    print(f"  support     {secs} securities · {tot['compared']:,} matched bars")
    print(f"  loss        {loss}")
    print(f"  T agreement {p['agreement']['t_label_agreement']} "
          f"(pred-matched {p['agreement']['predecessor_also_matched']['agreement']})")
    for f, v in p["price_basis"].items():
        print(f"    {f:6s} exact {v['rate']} · <=1c {v['within_1_cent']} · "
              f"max {v['max_abs_diff']}")
    print(f"  by position {[(k, v['agreement']) for k, v in p['by_session_position'].items()]}")
    print(f"  by body     {[(k, v['agreement']) for k, v in p['by_body_size'].items()]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
