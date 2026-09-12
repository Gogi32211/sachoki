"""MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V1 — three comparisons, one support contract.

    A = legacy MATERIALIZED 15m t_sig            studio_15m.duckdb :: bars.t_sig
    B = canonical engine on the LEGACY 15m OHLC  (same store, so A and B share a feed)
    C = canonical engine on the MASSIVE 15m OHLC (T_MASSIVE_PORT semantics)

    A<->B  legacy materialized-storage conformance — the TRUE Layer 2, at 15m
    B<->C  same-engine source/input divergence
    A<->C  historical-materialized vs Massive-port population divergence

A AND B DELIBERATELY SHARE ONE STORE. Running B on a different 15m OHLC store than the one A was
written beside would fold a source difference into what is supposed to be a pure storage test.

TWO PREFLIGHT AMBIGUITIES WERE MEASURED, NOT ASSUMED. The legacy 15m store holds each ticker in
exactly ONE universe — zero duplicated (ticker, timestamp) groups across 90.9M rows — so no dedup
rule is required and none is invented; this is also why the historical reader's duplicate guard
never fired. And t_sig has no companion availability column: 0 NULLs, 48,637,504 empty strings,
42,310,566 labels. BLANK therefore conflates "evaluable, no T" with "could not evaluate", and it
is carried as its own state in every confusion matrix rather than silently equated with NONE.
Agreement is reported both ways so the assumption's cost is visible instead of buried.

FOUR DENOMINATORS, BECAUSE ONE WOULD LIE. A matched current bar does not mean both sides saw the
same predecessor, and T reads shift(1). S3 — current AND required predecessor matched and usable
on both sides — is the primary denominator for semantic comparison.

TIMESTAMPS ARE GUARDED BECAUSE THIS ALREADY BIT ONCE. A us/ms mix-up silently produced zero
matches in an earlier run that sealed anyway. Conversion is explicit, a unit-corruption fixture
runs every time, and zero matched support is a HOLD.

X-ONLY. No outcome, no Y, no parameter change, no label correction toward legacy.
"""
from __future__ import annotations
import glob, json, os, sys, time                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                        # noqa: E402
import pandas as pd                                                       # noqa: E402
import pyarrow.parquet as pq                                              # noqa: E402
import t5_artifact as ART                                                 # noqa: E402
from signal_engine import compute_signals                                 # noqa: E402

LEG = "/Users/sachoki/Desktop/sachoki-desktop/data/studio_15m.duckdb"
COARSE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1/15m"
BIND = {
    "MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json": "5ebcad54f619f97d",
    "MASSIVE_T_LEGACY_15M_AUTHORITY_CORRECTION_V1.json": "24ac4562a96aa869",
    "MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1.json": "4a19b83f40adc6cc",
    "MASSIVE_T_FEATURE_PRODUCTION_V1.json": "ccdb0e35490794da",
    "MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1.json": "2bc48ac417c2cbc9",
    "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json": "7c0a5ad6b17d7ae0",
}
T_SLICE, PORT_HASH = "2901107d9baa6320", "0ca86eeb1f8ee8fe"
ET = "America/New_York"
MPOS = {"09:30": "M1", "09:45": "M2", "10:00": "M3", "10:15": "M4"}


def to_ms(s):
    """Explicit ms since epoch, whatever the source resolution."""
    return (pd.to_datetime(s, utc=True).dt.tz_localize(None)
            .astype("datetime64[ms]").astype("int64").to_numpy())


def labels(o, h, l, c, idx):
    d = pd.DataFrame({"open": o, "high": h, "low": l, "close": c}, index=idx)
    s = compute_signals(d)
    bc = s["bc"].to_numpy().astype(int)
    return np.where(bc > 0, s["sig_name"].to_numpy(), "NONE")


class Acc:
    def __init__(self):
        self.n = 0; self.agree = 0; self.conf = {}

    def add(self, a, b):
        self.n += len(a); self.agree += int((a == b).sum())
        for x, y in zip(a[a != b], b[a != b]):
            k = f"{x}->{y}"; self.conf[k] = self.conf.get(k, 0) + 1

    def out(self):
        return dict(support=self.n, agreed=self.agree,
                    agreement=round(self.agree / self.n, 6) if self.n else None,
                    top_disagreements=dict(sorted(self.conf.items(),
                                                  key=lambda kv: -kv[1])[:20]))


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="three-way 15m conformance (read-only)")
    import duckdb
    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding mismatch:", bad); return 1
    inv = json.load(open("MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json"))
    if not inv["completeness_guard"]["enumeration_complete"]:
        print("HOLD — evidence surface not closed"); return 1

    # ---- timestamp unit-corruption fixture, every run ----------------------
    probe = pd.Series(pd.to_datetime(["2024-06-03 13:30:00"]))
    if to_ms(probe)[0] != 1717421400000:
        print("HOLD — timestamp conversion fixture failed"); return 1
    corrupted = to_ms(probe)[0] // 1000
    if corrupted == to_ms(probe)[0]:
        print("HOLD — unit-corruption fixture is inert"); return 1

    el = json.load(open("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    tickers = sorted(el["cohort"]["eligible_tickers"])
    want = {key_of[t]: t for t in tickers if t in key_of}

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
        if i % 400 == 0:
            print(f"    {i+1}/{len(files)}", flush=True)
    MV = {k: pd.concat(v, ignore_index=True).sort_values("bar_start")
          for k, v in acc.items()}
    del acc

    con = duckdb.connect(LEG, read_only=True)
    idmap = dict(mapped=0, unmapped=0, no_legacy_rows=0)
    S = dict(S0=0, S1=0, S2=0, S3=0)
    AB, BC, AC = Acc(), Acc(), Acc()
    AB_lab, AC_lab = Acc(), Acc()          # restricted to rows where A carries a label
    pos_acc = {m: dict(AB=Acc(), BC=Acc(), AC=Acc()) for m in
               list(MPOS.values()) + ["SESSION_FINAL", "OTHER"]}
    pair = {p: dict(support=0, both_same=0, one_diff=0, both_diff=0)
            for p in ("M1+M2", "M2+M3", "M3+M4")}
    ohlc = {f: dict(n=0, exact=0, d1c=0, d10c=0, big=0, mx=0.0) for f in
            ("open", "high", "low", "close")}
    a_blank_where_b_eval = 0
    t0 = time.time()

    for n_i, (k, tk) in enumerate(sorted(want.items(), key=lambda kv: kv[1])):
        if k not in MV:
            continue
        lg = con.execute(
            "select date, open, high, low, close, coalesce(t_sig,'') t_sig "
            "from bars where ticker = ? order by date", [tk]).fetchdf()
        if len(lg) < 3:
            idmap["no_legacy_rows"] += 1; continue
        idmap["mapped"] += 1
        lts = to_ms(lg["date"])
        m = MV[k]; mts = m["bar_start"].to_numpy()

        A_all = np.where(lg["t_sig"].to_numpy() != "", lg["t_sig"].to_numpy(), "BLANK")
        B_all = labels(lg["open"].to_numpy(float), lg["high"].to_numpy(float),
                       lg["low"].to_numpy(float), lg["close"].to_numpy(float),
                       pd.to_datetime(lts, unit="ms", utc=True))
        C_all = labels(m["open"].to_numpy(float), m["high"].to_numpy(float),
                       m["low"].to_numpy(float), m["close"].to_numpy(float),
                       pd.to_datetime(mts, unit="ms", utc=True))

        li = {t: j for j, t in enumerate(lts)}
        common = np.array([t in li for t in mts])
        S["S0"] += int(common.sum())
        okc = m["coverage_state"].to_numpy() == "COMPLETE"
        usable = common & okc
        S["S1"] += int(usable.sum())
        prev_common = np.zeros(len(mts), bool); prev_common[1:] = common[:-1]
        S["S2"] += int((usable & prev_common).sum())
        prev_ok = np.zeros(len(mts), bool); prev_ok[1:] = okc[:-1]
        use = usable & prev_common & prev_ok
        S["S3"] += int(use.sum())
        if not use.any():
            continue

        li_idx = np.array([li[t] for t in mts[use]])
        a = A_all[li_idx]; b = B_all[li_idx]; c = C_all[use]
        AB.add(a, b); BC.add(b, c); AC.add(a, c)
        lab = a != "BLANK"
        if lab.any():
            AB_lab.add(a[lab], b[lab]); AC_lab.add(a[lab], c[lab])
        a_blank_where_b_eval += int((~lab).sum())

        et = pd.to_datetime(mts[use], unit="ms", utc=True).tz_convert(ET).strftime("%H:%M")
        ii = m["interval_index"].to_numpy()[use]
        last = ii == ii.max()
        pos = np.array([MPOS.get(e, "OTHER") for e in et], dtype=object)
        pos[last & (pos == "OTHER")] = "SESSION_FINAL"
        for pname in pos_acc:
            sel = pos == pname
            if sel.any():
                pos_acc[pname]["AB"].add(a[sel], b[sel])
                pos_acc[pname]["BC"].add(b[sel], c[sel])
                pos_acc[pname]["AC"].add(a[sel], c[sel])

        # pair-level portability on A vs C, the population question
        day = pd.to_datetime(mts[use], unit="ms", utc=True).tz_convert(ET).strftime("%Y-%m-%d")
        dfp = pd.DataFrame(dict(day=day, pos=pos, same=(a == c)))
        for pname, (p1, p2) in (("M1+M2", ("M1", "M2")), ("M2+M3", ("M2", "M3")),
                                ("M3+M4", ("M3", "M4"))):
            g = dfp[dfp["pos"].isin((p1, p2))].groupby("day")["same"]
            cnt = g.count(); sm = g.sum()
            both = cnt == 2
            pair[pname]["support"] += int(both.sum())
            pair[pname]["both_same"] += int(((sm == 2) & both).sum())
            pair[pname]["one_diff"] += int(((sm == 1) & both).sum())
            pair[pname]["both_diff"] += int(((sm == 0) & both).sum())

        for f in ("open", "high", "low", "close"):
            la = lg[f].to_numpy(float)[li_idx]; mb = m[f].to_numpy(float)[use]
            dd = np.abs(la - np.round(mb, 2))
            e = ohlc[f]
            e["n"] += len(la)
            e["exact"] += int(np.isclose(la, np.round(mb, 2), rtol=0, atol=1e-9).sum())
            e["d1c"] += int((dd <= 0.010000001).sum())
            e["d10c"] += int((dd <= 0.100000001).sum())
            e["big"] += int((dd > 1.0).sum())
            e["mx"] = max(e["mx"], float(np.nanmax(dd)))
        if n_i % 60 == 0:
            print(f"    {n_i+1}/{len(want)} · S3 {S['S3']:,} · {time.time()-t0:.0f}s",
                  flush=True)
    con.close()

    if S["S3"] == 0:
        print("HOLD — zero matched support; a comparison that matches nothing is a harness "
              "defect, not a measurement"); return 1

    p = dict(
        report_id="MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V1",
        status="THREE_WAY_CONFORMANCE_COMPLETE",
        task_class="X_ONLY_CONFORMANCE_DIAGNOSTIC",
        objects=dict(
            A="legacy MATERIALIZED 15m t_sig (studio_15m.duckdb :: bars.t_sig)",
            B="canonical engine on the LEGACY 15m OHLC (same store as A)",
            C="canonical engine on the MASSIVE 15m OHLC (T_MASSIVE_PORT semantics)",
            why_A_and_B_share_a_store="running B on a different 15m OHLC store would fold a "
                                      "source difference into a pure storage test"),
        bindings=BIND, producer_semantics_digest=T_SLICE, port_hash=PORT_HASH,

        preflight=dict(
            legacy_dedup=dict(
                duplicated_ticker_timestamp_groups=0,
                finding="each ticker appears in exactly ONE universe in this store",
                consequence="no dedup rule is required and none was invented",
                explains="the historical reader's duplicate guard never fired",
                measured_not_assumed=True),
            legacy_availability=dict(
                t_sig_nulls=0, t_sig_empty=48637504, t_sig_labelled=42310566,
                availability_column="NONE (only wt_valid_tr, a WLNBB field)",
                consequence="BLANK conflates 'evaluable, no T' with 'could not evaluate'",
                handling="BLANK is carried as its OWN state in every confusion matrix and is "
                         "never silently equated with NONE",
                limitation="LEGACY_AVAILABILITY_AMBIGUITY"),
            timestamp_guard=dict(explicit_ms_conversion=True, fixture_passed=True,
                                 tolerance=0, nearest_match=False,
                                 zero_support_is_hold=True,
                                 why="a us/ms mix-up silently produced zero matches in an "
                                     "earlier run that sealed anyway")),

        identity=dict(key="security_key_v1 + exact 15m timestamp (UTC, bar-start)",
                      mapped=idmap["mapped"], unmapped=idmap["unmapped"],
                      no_legacy_rows=idmap["no_legacy_rows"],
                      heuristics_used=False),

        support=dict(
            S0_current_timestamp_matched=S["S0"],
            S1_current_matched_and_usable=S["S1"],
            S2_current_and_predecessor_matched=S["S2"],
            S3_current_and_predecessor_matched_and_usable_both=S["S3"],
            primary_denominator="S3",
            why="T reads shift(1); a matched current bar does not mean both sides saw the "
                "same predecessor"),

        A_vs_B_legacy_materialized_storage_conformance=dict(
            **AB.out(),
            label_restricted=AB_lab.out(),
            blank_rows_where_engine_evaluable=a_blank_where_b_eval,
            classification="DIAGNOSTIC_ONLY",
            question="how faithfully does current canonical semantics explain the historical "
                     "materialized 15m T surface?"),
        B_vs_C_source_divergence=dict(
            **BC.out(), classification="DIAGNOSTIC_ONLY",
            question="how much does the X source alter T under identical semantics?"),
        A_vs_C_population_divergence=dict(
            **AC.out(), label_restricted=AC_lab.out(),
            classification="DIAGNOSTIC_ONLY",
            question="how different is the actual historical materialized population from the "
                     "Massive research population?",
            IS_NOT_PURE_SOURCE_DIVERGENCE=True,
            carries=["legacy materialization/storage residual", "source OHLC divergence",
                     "input-vintage differences", "identity/dedup differences",
                     "unresolved residual"],
            forbidden_statement="'Massive source causes X% divergence' — that is B<->C"),

        additivity_warning=dict(
            rule="A<->C divergence != A<->B divergence + B<->C divergence",
            why="state disagreements overlap and interact non-linearly; these are a "
                "diagnostic decomposition BY COMPARISON, not an additive variance "
                "decomposition"),

        by_session_position={k: {c: v[c].out()["agreement"] for c in ("AB", "BC", "AC")}
                             for k, v in pos_acc.items()},
        registered_position_pairs=pair,
        pair_note="X-side portability of the historical position-pair claims; no Y, no claim "
                  "statistic",

        price_basis={f: dict(compared=v["n"],
                             exact_after_round=round(v["exact"] / v["n"], 6) if v["n"] else None,
                             within_1_cent=round(v["d1c"] / v["n"], 6) if v["n"] else None,
                             within_10_cents=round(v["d10c"] / v["n"], 6) if v["n"] else None,
                             above_1_dollar=round(v["big"] / v["n"], 6) if v["n"] else None,
                             max_abs_diff=v["mx"]) for f, v in ohlc.items()},
        input_vintage_attribution=dict(status="PLAUSIBLE / STRONGLY_SUGGESTED",
                                       confirmed=False,
                                       what_would_confirm="joining a corporate-action / "
                                                          "split / spin-off authority at row "
                                                          "level"),

        same_day_anchor_not_mixed_in=dict(
            artifact="MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1",
            digest="819329bf48bd7175",
            note="this gate measures X portability only; no statement here connects position "
                 "divergence to any outcome, and Y does not exist in this programme"),

        no_parameter_changes=0, no_source_correction=True,
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        elapsed_sec=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V1.json",
                 required=("report_id", "status", "objects", "preflight", "support",
                           "A_vs_B_legacy_materialized_storage_conformance",
                           "B_vs_C_source_divergence", "A_vs_C_population_divergence",
                           "additivity_warning"),
                 supersede=os.path.exists(
                     "MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V1.json"))
    print(f"\nMASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V1 · {d} · {p['status']}")
    print(f"  support     S0 {S['S0']:,} · S1 {S['S1']:,} · S2 {S['S2']:,} · S3 {S['S3']:,}")
    print(f"  A<->B  storage    {AB.out()['agreement']}   "
          f"(label-restricted {AB_lab.out()['agreement']})")
    print(f"  B<->C  source     {BC.out()['agreement']}")
    print(f"  A<->C  population {AC.out()['agreement']}   "
          f"(label-restricted {AC_lab.out()['agreement']})")
    print(f"  by position (AB / BC / AC):")
    for k, v in p["by_session_position"].items():
        print(f"    {k:14s} {v['AB']} / {v['BC']} / {v['AC']}")
    print(f"  pairs       {pair}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
