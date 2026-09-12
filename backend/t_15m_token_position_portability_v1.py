"""MASSIVE_T_LEGACY_15M_TOKEN_POSITION_PORTABILITY_V1 — the position/pair slices, made readable.

The three-way gate's position and pair figures were dominated by a representational artefact:
legacy blank compared against the engine's NONE counted as a mismatch on 4.7M rows, so every
slice sat near 50% and measured nothing about portability. This amendment recomputes them under
an explicit token normalization.

THE NORMALIZATION IS DELIBERATELY WEAKER THAN IT COULD BE. Legacy blank becomes NO_T_LABEL —
"no materialized T label" — and NOT "evaluable, no T state". The legacy 15m store carries no
availability column, so blank cannot distinguish "the engine ran and nothing fired" from "this
bar could not be evaluated". Reading it as the former would extract stronger semantics from a
representation than the representation contains, which is the exact error class this programme
has already committed three times. So what is measured here is MATERIALIZED-LABEL PORTABILITY,
never availability-semantic equivalence.

PAIRS ARE THE POINT. The registered historical claims live at position-pair level — M1->M2,
M2->M3, M3->M4 — so a pair is only comparable when BOTH of its bars are jointly usable, and a
pair is only portable when BOTH tokens carry across. First- and second-position mismatches are
counted separately, because a pair claim can fail from either end and the two are not
interchangeable.

X-ONLY. No outcome, no edge interpretation, no Y.
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
THREE, THREE_D = "MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V1.json", "715765f2be4b8711"
BIND = {THREE: THREE_D,
        "MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json": "5ebcad54f619f97d",
        "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json": "7c0a5ad6b17d7ae0"}
ET = "America/New_York"
MPOS = {"09:30": "M1", "09:45": "M2", "10:00": "M3", "10:15": "M4"}
PAIRS = (("M1+M2", "M1", "M2"), ("M2+M3", "M2", "M3"), ("M3+M4", "M3", "M4"))


def to_ms(s):
    return (pd.to_datetime(s, utc=True).dt.tz_localize(None)
            .astype("datetime64[ms]").astype("int64").to_numpy())


def eng_tokens(o, h, l, c, idx):
    d = pd.DataFrame({"open": o, "high": h, "low": l, "close": c}, index=idx)
    s = compute_signals(d)
    bc = s["bc"].to_numpy().astype(int)
    return np.where(bc > 0, s["sig_name"].to_numpy(), "NO_T_LABEL")


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="token position portability (read-only)")
    import duckdb
    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding mismatch:", bad); return 1

    el = json.load(open("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    want = {key_of[t]: t for t in sorted(el["cohort"]["eligible_tickers"]) if t in key_of}

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
    MV = {k: pd.concat(v, ignore_index=True).sort_values("bar_start") for k, v in acc.items()}
    del acc

    con = duckdb.connect(LEG, read_only=True)
    pos_names = list(MPOS.values()) + ["SESSION_FINAL", "OTHER"]
    pos = {p: dict(AC_n=0, AC_ok=0, AB_n=0, AB_ok=0, BC_n=0, BC_ok=0) for p in pos_names}
    pair = {p[0]: dict(comparable=0, both_same=0, exactly_one_differs=0, both_differ=0,
                       first_only=0, second_only=0) for p in PAIRS}
    tot = dict(S3=0, AC_ok=0, AB_ok=0, BC_ok=0)
    t0 = time.time()

    for n_i, (k, tk) in enumerate(sorted(want.items(), key=lambda kv: kv[1])):
        if k not in MV:
            continue
        lg = con.execute("select date, open, high, low, close, coalesce(t_sig,'') t_sig "
                         "from bars where ticker = ? order by date", [tk]).fetchdf()
        if len(lg) < 3:
            continue
        lts = to_ms(lg["date"]); m = MV[k]; mts = m["bar_start"].to_numpy()
        raw = lg["t_sig"].to_numpy()
        A_all = np.where(raw != "", raw, "NO_T_LABEL")          # blank -> NO_T_LABEL
        B_all = eng_tokens(lg["open"].to_numpy(float), lg["high"].to_numpy(float),
                           lg["low"].to_numpy(float), lg["close"].to_numpy(float),
                           pd.to_datetime(lts, unit="ms", utc=True))
        C_all = eng_tokens(m["open"].to_numpy(float), m["high"].to_numpy(float),
                           m["low"].to_numpy(float), m["close"].to_numpy(float),
                           pd.to_datetime(mts, unit="ms", utc=True))

        li = {t: j for j, t in enumerate(lts)}
        common = np.array([t in li for t in mts])
        okc = m["coverage_state"].to_numpy() == "COMPLETE"
        prev_common = np.zeros(len(mts), bool); prev_common[1:] = common[:-1]
        prev_ok = np.zeros(len(mts), bool); prev_ok[1:] = okc[:-1]
        use = common & okc & prev_common & prev_ok
        if not use.any():
            continue
        idx = np.array([li[t] for t in mts[use]])
        a = A_all[idx]; b = B_all[idx]; c = C_all[use]
        tot["S3"] += len(a)
        tot["AC_ok"] += int((a == c).sum()); tot["AB_ok"] += int((a == b).sum())
        tot["BC_ok"] += int((b == c).sum())

        ts = pd.to_datetime(mts[use], unit="ms", utc=True).tz_convert(ET)
        et = ts.strftime("%H:%M"); day = ts.strftime("%Y-%m-%d")
        ii = m["interval_index"].to_numpy()[use]; last = ii == ii.max()
        pv = np.array([MPOS.get(e, "OTHER") for e in et], dtype=object)
        pv[last & (pv == "OTHER")] = "SESSION_FINAL"
        for pn in pos_names:
            sel = pv == pn
            if sel.any():
                pos[pn]["AC_n"] += int(sel.sum()); pos[pn]["AC_ok"] += int((a[sel] == c[sel]).sum())
                pos[pn]["AB_n"] += int(sel.sum()); pos[pn]["AB_ok"] += int((a[sel] == b[sel]).sum())
                pos[pn]["BC_n"] += int(sel.sum()); pos[pn]["BC_ok"] += int((b[sel] == c[sel]).sum())

        df = pd.DataFrame(dict(day=day, pos=pv, same=(a == c)))
        for pname, p1, p2 in PAIRS:
            sub = df[df["pos"].isin((p1, p2))]
            if sub.empty:
                continue
            w = sub.pivot_table(index="day", columns="pos", values="same", aggfunc="first")
            if p1 not in w.columns or p2 not in w.columns:
                continue
            w = w.dropna(subset=[p1, p2])
            f1 = w[p1].to_numpy().astype(bool); f2 = w[p2].to_numpy().astype(bool)
            e = pair[pname]
            e["comparable"] += len(w)
            e["both_same"] += int((f1 & f2).sum())
            e["both_differ"] += int((~f1 & ~f2).sum())
            e["exactly_one_differs"] += int((f1 ^ f2).sum())
            e["first_only"] += int((~f1 & f2).sum())
            e["second_only"] += int((f1 & ~f2).sum())
        if n_i % 80 == 0:
            print(f"    {n_i+1}/{len(want)} · S3 {tot['S3']:,} · {time.time()-t0:.0f}s",
                  flush=True)
    con.close()
    if tot["S3"] == 0:
        print("HOLD — zero support"); return 1

    r = lambda n, d: round(n / d, 6) if d else None                     # noqa: E731
    p = dict(
        report_id="MASSIVE_T_LEGACY_15M_TOKEN_POSITION_PORTABILITY_V1",
        status="TOKEN_POSITION_PORTABILITY_MEASURED",
        task_class="X_ONLY_DIAGNOSTIC_AMENDMENT",
        amends=dict(artifact="MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V1",
                    digest=THREE_D,
                    disposition="MEASUREMENTS VALID / INTERPRETATION INCOMPLETE — not edited",
                    what_was_wrong="its position and pair slices were dominated by the "
                                   "blank-vs-NONE representational artefact and measured "
                                   "nothing about portability"),

        normalization=dict(
            A={"T1..T12": "same T token", "''": "NO_T_LABEL"},
            B_and_C={"T1..T12": "same T token", "engine NONE": "NO_T_LABEL"},
            legacy_blank_means="NO_MATERIALIZED_T_LABEL",
            legacy_blank_does_NOT_mean="EVALUABLE_NO_T_STATE",
            why="the legacy 15m store carries no availability column, so blank cannot "
                "distinguish 'nothing fired' from 'could not be evaluated'; reading it as the "
                "former would extract stronger semantics than the representation contains",
            measures="MATERIALIZED_LABEL_PORTABILITY",
            does_not_measure="AVAILABILITY_SEMANTIC_EQUIVALENCE"),

        support=dict(primary_denominator="S3 — current and required predecessor matched and "
                                         "usable on both sides", bars=tot["S3"]),
        overall_normalized=dict(
            A_vs_C=r(tot["AC_ok"], tot["S3"]),
            A_vs_B=r(tot["AB_ok"], tot["S3"]),
            B_vs_C=r(tot["BC_ok"], tot["S3"]),
            permitted_statement="on jointly usable support, after normalizing legacy blank to "
                                "'no materialized T label', Massive and legacy materialized T "
                                "tokens agree at this rate",
            forbidden_statement="'T_MASSIVE_PORT reproduces the legacy materialized 15m T "
                                "population' — legacy blank does not encode enough "
                                "information to establish availability-semantic equivalence"),

        by_position={k: dict(bars=v["AC_n"], A_vs_C=r(v["AC_ok"], v["AC_n"]),
                             A_vs_B=r(v["AB_ok"], v["AB_n"]),
                             B_vs_C=r(v["BC_ok"], v["BC_n"])) for k, v in pos.items()},

        registered_position_pairs={
            k: dict(**v, pair_portability=r(v["both_same"], v["comparable"]))
            for k, v in pair.items()},
        pair_definition=dict(
            comparable="both bars of the pair jointly usable on the same session date",
            portable="BOTH tokens carry across",
            first_only="only the first position's token differs",
            second_only="only the second position's token differs",
            why_separate="a pair claim can fail from either end and the two are not "
                         "interchangeable"),

        no_y=True, no_edge_interpretation=True,
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        elapsed_sec=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_LEGACY_15M_TOKEN_POSITION_PORTABILITY_V1.json",
                 required=("report_id", "status", "normalization", "overall_normalized",
                           "by_position", "registered_position_pairs"),
                 supersede=os.path.exists(
                     "MASSIVE_T_LEGACY_15M_TOKEN_POSITION_PORTABILITY_V1.json"))
    print(f"\nMASSIVE_T_LEGACY_15M_TOKEN_POSITION_PORTABILITY_V1 · {d} · {p['status']}")
    print(f"  support S3  {tot['S3']:,}")
    print(f"  normalized  A<->C {p['overall_normalized']['A_vs_C']} · "
          f"A<->B {p['overall_normalized']['A_vs_B']} · "
          f"B<->C {p['overall_normalized']['B_vs_C']}")
    print("  by position (bars · A<->C · A<->B · B<->C):")
    for k, v in p["by_position"].items():
        print(f"    {k:14s} {v['bars']:>9,}  {v['A_vs_C']}  {v['A_vs_B']}  {v['B_vs_C']}")
    print("  registered pairs:")
    for k, v in p["registered_position_pairs"].items():
        print(f"    {k}  comparable {v['comparable']:,} · portable {v['pair_portability']} "
              f"· one-differs {v['exactly_one_differs']:,} · both-differ {v['both_differ']:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
