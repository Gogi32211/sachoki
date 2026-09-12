"""MASSIVE_T_LEGACY_15M_OHLC_VINTAGE_DIAGNOSTIC_V1 — attribution, or an honest UNRESOLVED.

A pattern is not a provenance fact. The three-way gate showed A tracking C while B — the engine
on the legacy store's own current OHLC — is the outlier, with A<->B disagreements mirroring
B<->C ones state-for-state. The obvious reading is that the legacy OHLC was revised after t_sig
was materialized. This gate exists to test that rather than adopt it because the mirror is
elegant.

CANDIDATES ARE FROZEN BEFORE ANY AGREEMENT IS COMPUTED. The enumeration is already closed, so
the candidate vintages are declared up front from the inventory. Sweeping vintages and then
keeping whichever best matches A would be fitting dressed as provenance, and it is forbidden
here in writing.

THE DECISIVE EVIDENCE IS NOT AN AGGREGATE RATE. If the label surface is stable while the OHLC
surface moved, and the rows whose OHLC moved are the SAME rows where A and B disagree, then the
revision hypothesis is carrying real weight. If the archived OHLC reproduces A no better than
the current one does, the hypothesis fails and the finding stays UNRESOLVED — with the
alternative explanations named rather than quietly dropped: a different upstream feed at
materialization time, partially different engine provenance, adjustment semantics, or a
transform not yet known.

SAMPLED, AND THE SAMPLE IS DECLARED. Row-level intersection needs full per-security history, so
this runs on a declared deterministic sample rather than the whole cohort, and says so.

X-ONLY. No Y, no parameter change, no feed selection for research.
"""
from __future__ import annotations
import glob, json, os, sys, time                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                        # noqa: E402
import pandas as pd                                                       # noqa: E402
import pyarrow.parquet as pq                                              # noqa: E402
import t5_artifact as ART                                                 # noqa: E402
from signal_engine import compute_signals                                 # noqa: E402

CUR = "/Users/sachoki/Desktop/sachoki-desktop/data/studio_15m.duckdb"
ARCH = ("/Volumes/QUANT_RESEARCH/archive/studio_pre_external_migration_2026-08-24/"
        "studio_15m.duckdb")
COARSE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1/15m"
BIND = {"MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V1.json": "715765f2be4b8711",
        "MASSIVE_T_LEGACY_15M_TOKEN_POSITION_PORTABILITY_V1.json": "f93999b9d10e0b0d",
        "MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json": "5ebcad54f619f97d",
        "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json": "7c0a5ad6b17d7ae0"}
SAMPLE_STRIDE, SAMPLE_N = 8, 60


def to_ms(s):
    return (pd.to_datetime(s, utc=True).dt.tz_localize(None)
            .astype("datetime64[ms]").astype("int64").to_numpy())


def tok(o, h, l, c, idx):
    d = pd.DataFrame({"open": o, "high": h, "low": l, "close": c}, index=idx)
    s = compute_signals(d)
    bc = s["bc"].to_numpy().astype(int)
    return np.where(bc > 0, s["sig_name"].to_numpy(), "NO_T_LABEL")


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="OHLC vintage diagnostic (read-only)")
    import duckdb
    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding mismatch:", bad); return 1
    if not os.path.exists(ARCH):
        print("HOLD — the archived 15m vintage is not present"); return 1

    el = json.load(open("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    ticks = sorted(el["cohort"]["eligible_tickers"])
    sample = ticks[::SAMPLE_STRIDE][:SAMPLE_N]
    keys = {key_of[t]: t for t in sample if t in key_of}

    print(f"  loading Massive 15m for {len(keys)} sampled securities …", flush=True)
    acc = {}
    for i, f in enumerate(sorted(glob.glob(os.path.join(COARSE, "*", "*", "*.parquet")))):
        d = pq.read_table(f, columns=["security_key_v1", "bar_start", "coverage_state",
                                      "open", "high", "low", "close"]).to_pandas()
        d = d[d["security_key_v1"].isin(keys)]
        if len(d):
            for k, g in d.groupby("security_key_v1", sort=False):
                acc.setdefault(k, []).append(g)
    MV = {k: pd.concat(v, ignore_index=True).sort_values("bar_start") for k, v in acc.items()}
    del acc

    cc = duckdb.connect(CUR, read_only=True)
    ca = duckdb.connect(ARCH, read_only=True)
    T = dict(common=0, tsig_same=0, ohlc_same=0,
             AcurB_cur=0, AcurE_arch=0, AcurC=0, n_eval=0,
             ab_mismatch=0, ohlc_changed=0, mismatch_and_changed=0,
             mismatch_not_changed=0, changed_not_mismatch=0)
    t0 = time.time()

    for n_i, (k, tk) in enumerate(sorted(keys.items(), key=lambda kv: kv[1])):
        q = ("select date, open, high, low, close, coalesce(t_sig,'') t_sig "
             "from bars where ticker = ? order by date")
        cu = cc.execute(q, [tk]).fetchdf()
        ar = ca.execute(q, [tk]).fetchdf()
        if len(cu) < 3 or len(ar) < 3:
            continue
        cts, ats = to_ms(cu["date"]), to_ms(ar["date"])
        ai = {t: j for j, t in enumerate(ats)}
        both = np.array([t in ai for t in cts])
        if not both.any():
            continue
        aidx = np.array([ai[t] for t in cts[both]])

        a_cur = np.where(cu["t_sig"].to_numpy() != "", cu["t_sig"].to_numpy(), "NO_T_LABEL")
        a_arc = np.where(ar["t_sig"].to_numpy() != "", ar["t_sig"].to_numpy(), "NO_T_LABEL")
        T["common"] += int(both.sum())
        T["tsig_same"] += int((a_cur[both] == a_arc[aidx]).sum())
        same_ohlc = np.ones(int(both.sum()), bool)
        for f in ("open", "high", "low", "close"):
            same_ohlc &= np.isclose(cu[f].to_numpy(float)[both],
                                    ar[f].to_numpy(float)[aidx], rtol=0, atol=1e-9)
        T["ohlc_same"] += int(same_ohlc.sum())

        B_cur = tok(cu["open"].to_numpy(float), cu["high"].to_numpy(float),
                    cu["low"].to_numpy(float), cu["close"].to_numpy(float),
                    pd.to_datetime(cts, unit="ms", utc=True))
        E_arc = tok(ar["open"].to_numpy(float), ar["high"].to_numpy(float),
                    ar["low"].to_numpy(float), ar["close"].to_numpy(float),
                    pd.to_datetime(ats, unit="ms", utc=True))

        # Massive side, on the same timestamps
        C_tok = None
        if k in MV:
            m = MV[k]; mts = m["bar_start"].to_numpy()
            C_all = tok(m["open"].to_numpy(float), m["high"].to_numpy(float),
                        m["low"].to_numpy(float), m["close"].to_numpy(float),
                        pd.to_datetime(mts, unit="ms", utc=True))
            mi = {t: j for j, t in enumerate(mts)}
            C_tok = np.array([C_all[mi[t]] if t in mi else None for t in cts[both]],
                             dtype=object)

        a_c = a_cur[both]; b_c = B_cur[both]; e_a = E_arc[aidx]
        T["n_eval"] += len(a_c)
        T["AcurB_cur"] += int((a_c == b_c).sum())
        T["AcurE_arch"] += int((a_c == e_a).sum())
        if C_tok is not None:
            ok = C_tok != None                                       # noqa: E711
            T["AcurC"] += int((a_c[ok] == C_tok[ok]).sum())

        mism = a_c != b_c
        chg = ~same_ohlc
        T["ab_mismatch"] += int(mism.sum())
        T["ohlc_changed"] += int(chg.sum())
        T["mismatch_and_changed"] += int((mism & chg).sum())
        T["mismatch_not_changed"] += int((mism & ~chg).sum())
        T["changed_not_mismatch"] += int((chg & ~mism).sum())
        if n_i % 15 == 0:
            print(f"    {n_i+1}/{len(keys)} · common {T['common']:,} · "
                  f"{time.time()-t0:.0f}s", flush=True)
    cc.close(); ca.close()
    if T["common"] == 0:
        print("HOLD — zero common support"); return 1

    r = lambda n, d: round(n / d, 6) if d else None                    # noqa: E731
    label_stable = r(T["tsig_same"], T["common"])
    ohlc_stable = r(T["ohlc_same"], T["common"])
    a_vs_cur = r(T["AcurB_cur"], T["n_eval"])
    a_vs_arch = r(T["AcurE_arch"], T["n_eval"])
    archive_better = (a_vs_arch or 0) > (a_vs_cur or 0)
    coincidence = r(T["mismatch_and_changed"], T["ab_mismatch"])

    supported = bool(label_stable is not None and label_stable > 0.999
                     and (ohlc_stable or 1) < 0.999 and archive_better
                     and (coincidence or 0) > 0.5)
    status = ("LEGACY_OHLC_POST_MATERIALIZATION_REVISION_SUPPORTED" if supported
              else "UNRESOLVED")

    p = dict(
        report_id="MASSIVE_T_LEGACY_15M_OHLC_VINTAGE_DIAGNOSTIC_V1",
        status=status,
        task_class="X_ONLY_PROVENANCE_ATTRIBUTION_DIAGNOSTIC",
        hypothesis="the legacy 15m OHLC was revised AFTER t_sig was materialized, which is "
                   "why the canonical engine on the store's current OHLC reproduces that "
                   "store's own labels worse than the Massive feed does",
        purpose="attribution — explicitly NOT selecting the best-matching feed for research",

        candidates_frozen_before_measurement=dict(
            V_CURRENT=CUR, V_ARCHIVE_20260824=ARCH,
            V_MASSIVE="reference source-port, not a legacy-vintage candidate",
            enumerated_from="MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1 (enumeration complete)",
            forbidden="sweeping vintages and keeping whichever best matches A — that is "
                      "fitting dressed as provenance"),

        sample=dict(securities=len(keys), stride=SAMPLE_STRIDE, cap=SAMPLE_N,
                    deterministic=True,
                    why_sampled="row-level intersection needs full per-security history",
                    declared=True),

        label_surface=dict(common_bars=T["common"], identical=T["tsig_same"],
                           stability=label_stable,
                           reading="LABEL_SURFACE_STABLE" if (label_stable or 0) > 0.999
                                   else "LABEL_SURFACE_ALSO_MOVED"),
        ohlc_surface=dict(common_bars=T["common"], identical=T["ohlc_same"],
                          stability=ohlc_stable,
                          reading="OHLC_SURFACE_CHANGED" if (ohlc_stable or 1) < 0.999
                                  else "OHLC_SURFACE_STABLE"),

        engine_vs_materialized=dict(
            A_vs_engine_on_CURRENT_ohlc=a_vs_cur,
            A_vs_engine_on_ARCHIVED_ohlc=a_vs_arch,
            archive_reproduces_better=archive_better,
            A_vs_massive=r(T["AcurC"], T["n_eval"]),
            bars=T["n_eval"]),

        row_level_coincidence=dict(
            ab_mismatch_rows=T["ab_mismatch"],
            ohlc_changed_rows=T["ohlc_changed"],
            mismatch_AND_changed=T["mismatch_and_changed"],
            mismatch_NOT_changed=T["mismatch_not_changed"],
            changed_NOT_mismatch=T["changed_not_mismatch"],
            share_of_mismatches_on_changed_rows=coincidence,
            why_it_matters="if the mirror lived only in aggregate counts it would be weak "
                           "evidence; the same ROWS moving is what carries weight"),

        verdict=dict(
            status=status,
            criteria=dict(label_surface_stable=(label_stable or 0) > 0.999,
                          ohlc_surface_changed=(ohlc_stable or 1) < 0.999,
                          archive_reproduces_A_better=archive_better,
                          majority_of_mismatches_on_changed_rows=(coincidence or 0) > 0.5),
            if_unresolved_alternatives=[
                "the historical producer consumed a different upstream feed vintage",
                "the historical materializer read a different OHLC surface entirely",
                "engine provenance differs in part",
                "rounding / corporate-action adjustment semantics",
                "a source transform not yet identified"],
            not_chosen_on="the elegance of the mirror pattern"),

        does_not_authorize=["selecting a legacy feed for research use",
                            "correcting Massive labels toward any legacy vintage",
                            "treating any vintage as the canonical X source"],
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        elapsed_sec=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_LEGACY_15M_OHLC_VINTAGE_DIAGNOSTIC_V1.json",
                 required=("report_id", "status", "hypothesis",
                           "candidates_frozen_before_measurement", "label_surface",
                           "ohlc_surface", "engine_vs_materialized",
                           "row_level_coincidence", "verdict"),
                 supersede=os.path.exists(
                     "MASSIVE_T_LEGACY_15M_OHLC_VINTAGE_DIAGNOSTIC_V1.json"))
    print(f"\nMASSIVE_T_LEGACY_15M_OHLC_VINTAGE_DIAGNOSTIC_V1 · {d} · {status}")
    print(f"  sample      {len(keys)} securities · common bars {T['common']:,}")
    print(f"  label surf  stability {label_stable}  -> {p['label_surface']['reading']}")
    print(f"  ohlc surf   stability {ohlc_stable}  -> {p['ohlc_surface']['reading']}")
    print(f"  A vs engine(current)  {a_vs_cur}")
    print(f"  A vs engine(archive)  {a_vs_arch}   better: {archive_better}")
    print(f"  A vs Massive          {r(T['AcurC'], T['n_eval'])}")
    print(f"  row coincidence  mismatches {T['ab_mismatch']:,} · on changed rows "
          f"{T['mismatch_and_changed']:,} ({coincidence})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
