"""MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2_REQUALIFIED — resealed on qualified support.

V2 held on 3 of 15,647,361 rows. The boundary investigation (38e044124d4f21ad) then established
a mechanism for all three, so this reseal computes the same comparisons on a QUALIFIED
materializer support. It does not overwrite V2; V2 stays as the HOLD it was.

THE EXCLUSION IS CAUSAL, NOT A BLACKLIST. A row may leave the materializer-conformance
denominator ONLY when it is bound to a sealed CONFIRMED_MATERIALIZATION_BOUNDARY_ANOMALY
authority. Any A!=B mismatch without such authority is a HOLD — so a future mismatch cannot
become an exception by resembling this one. That guard is exercised here, not merely described.

AND THE EXCLUSION IS SCOPED. These three rows are a LEGACY MATERIALIZATION defect, not a Massive
input defect, so they leave the A<->B denominator and nothing else. They are not removed from the
Massive research population, and B<->C — which never reads stored A at all — keeps its own
support rules untouched.

V1 fed B the enriched store's own OHLC column on the assumption that a label and an OHLC column
in one table implied that column produced that label. The provenance resolution
(5bb9cdc3167e2ff7) showed the real input is the LEAN base, and that the canonical engine on it
reproduces the stored t_sig EXACTLY. So V1's A<->B and B<->C were measuring the wrong thing and
are recomputed here rather than reinterpreted.

    A = studio_15m.duckdb :: bars.t_sig            historical materialized label surface
    B = canonical engine( studio_15m_base OHLC )   the TRUE historical input surface
    C = canonical engine( Massive 15m OHLC )       the accepted T_MASSIVE_PORT

TWO INVARIANTS, NOT TWO RATES. A<->B is now an EXACT identity check, not a conformance score —
a single mismatch on the full applicable population is a HOLD, and inventing a tolerance to
absorb it is forbidden. And because A == B, A<->C and B<->C must disagree on exactly the SAME
rows with exactly the same confusion matrix when computed over one shared support; if they do
not, the harness has a support-alignment defect and that is what the run has discovered.

FOUR DENOMINATORS, EACH BOUND TO ITS COMPARISON, so that no two rates in this artifact can be
read side by side without their populations. L0 carries A<->B. L3 — current and required
predecessor aligned and usable on every side — carries the source-port comparisons.

BLANK IS UNCHANGED BY THE PROVENANCE RESULT. Legacy '' still means NO_MATERIALIZED_T_LABEL and
still cannot mean EVALUABLE_NO_T_STATE; the store has no availability column and knowing who
wrote it does not add one. Raw and token-normalized layers are both reported.
"""
from __future__ import annotations
import glob, json, os, sys, time                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                        # noqa: E402
import pandas as pd                                                       # noqa: E402
import pyarrow.parquet as pq                                              # noqa: E402
import t5_artifact as ART                                                 # noqa: E402
from signal_engine import compute_signals                                 # noqa: E402

ENR = "/Users/sachoki/Desktop/sachoki-desktop/data/studio_15m.duckdb"
BASE = "/Volumes/QUANT_RESEARCH/source_data/studio/studio_15m_base.duckdb"
COARSE = "/Volumes/QUANT_RESEARCH/studio_data/derived/massive_coarse_v1/15m"
BOUND, BOUND_D = ("MASSIVE_T_LEGACY_15M_TRANSITION_BOUNDARY_INVESTIGATION_V1.json",
                  "38e044124d4f21ad")
BIND = {BOUND: BOUND_D,
        "MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2.json": "15fcd9db286944e2",
        "MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1.json": "5bb9cdc3167e2ff7",
        "MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json": "5ebcad54f619f97d",
        "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json": "7c0a5ad6b17d7ae0"}
ET = "America/New_York"
MPOS = {"09:30": "M1", "09:45": "M2", "10:00": "M3", "10:15": "M4"}
PAIRS = (("M1+M2", "M1", "M2"), ("M2+M3", "M2", "M3"), ("M3+M4", "M3", "M4"))


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
    require_external_volume(purpose="three-way conformance V2 (read-only)")
    import duckdb
    bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
    if bad:
        print("HOLD — binding mismatch:", bad); return 1

    bnd = json.load(open(BOUND))
    if bnd["status"] != "LOCALIZED_MATERIALIZATION_BOUNDARY_ANOMALY_CONFIRMED":
        print("HOLD — boundary authority is not CONFIRMED"); return 1
    COVERED_TS = bnd["exclusion_rule"]["timestamp"]
    COVERED_TK = set(bnd["exclusion_rule"]["tickers"])
    COVERED_MS = int(pd.Timestamp(COVERED_TS).value // 10**6)

    el = json.load(open("SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json"))
    ids = json.load(open("MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"))["mapping"]
    key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in ids}
    want = {key_of[t]: t for t in sorted(el["cohort"]["eligible_tickers"]) if t in key_of}

    print("  loading Massive 15m …", flush=True)
    acc = {}
    for i, f in enumerate(sorted(glob.glob(os.path.join(COARSE, "*", "*", "*.parquet")))):
        d = pq.read_table(f, columns=["security_key_v1", "bar_start", "interval_index",
                                      "coverage_state", "open", "high", "low",
                                      "close"]).to_pandas()
        d = d[d["security_key_v1"].isin(want)]
        for k, g in d.groupby("security_key_v1", sort=False):
            acc.setdefault(k, []).append(g)
        if i % 400 == 0:
            print(f"    {i+1}/1254", flush=True)
    MV = {k: pd.concat(v, ignore_index=True).sort_values("bar_start") for k, v in acc.items()}
    del acc

    ce = duckdb.connect(ENR, read_only=True)
    cb = duckdb.connect(BASE, read_only=True)
    L = dict(L0=0, L1=0, L2=0, L3=0)
    ab_mismatch = 0
    ab_excluded = 0
    ab_uncovered = 0
    ab_examples = []
    AC_ok = BC_ok = 0
    row_identical = True
    conf_AC, conf_BC = {}, {}
    pos_names = list(MPOS.values()) + ["SESSION_FINAL", "OTHER"]
    pos = {p: dict(n=0, ac=0, bc=0) for p in pos_names}
    pair = {p[0]: dict(comparable=0, both_same=0, first_only=0, second_only=0,
                       both_differ=0) for p in PAIRS}
    raw_blank_vs_C = {}
    t0 = time.time()

    for n_i, (k, tk) in enumerate(sorted(want.items(), key=lambda kv: kv[1])):
        e = ce.execute("select date, coalesce(t_sig,'') t_sig from bars where ticker = ? "
                       "order by date", [tk]).fetchdf()
        b = cb.execute("select date, open, high, low, close from bars where ticker = ? "
                       "order by date", [tk]).fetchdf()
        if len(e) < 3 or len(b) < 3:
            continue
        ets, bts = to_ms(e["date"]), to_ms(b["date"])
        A_all = np.where(e["t_sig"].to_numpy() != "", e["t_sig"].to_numpy(), "NO_T_LABEL")
        B_all = tok(b["open"].to_numpy(float), b["high"].to_numpy(float),
                    b["low"].to_numpy(float), b["close"].to_numpy(float),
                    pd.to_datetime(bts, unit="ms", utc=True))

        # ---- L0 : A<->B on every timestamp both stores hold -----------------
        bi = {t: j for j, t in enumerate(bts)}
        m0 = np.array([t in bi for t in ets])
        if m0.any():
            j0 = np.array([bi[t] for t in ets[m0]])
            a0, b0 = A_all[m0], B_all[j0]
            L["L0"] += len(a0)
            bad = a0 != b0
            ab_mismatch += int(bad.sum())
            # A row leaves the denominator ONLY if a sealed authority covers it.
            covered = (tk in COVERED_TK) & (ets[m0] == COVERED_MS)
            excluded_here = bad & covered
            ab_excluded += int(excluded_here.sum())
            ab_uncovered += int((bad & ~covered).sum())
            if bad.any() and len(ab_examples) < 8:
                w = np.where(bad)[0][:3]
                for x in w:
                    ab_examples.append(dict(ticker=tk,
                                            ts=str(pd.to_datetime(ets[m0][x], unit="ms",
                                                                  utc=True)),
                                            A=str(a0[x]), B=str(b0[x])))
        if k not in MV:
            continue
        m = MV[k]; mts = m["bar_start"].to_numpy()
        C_all = tok(m["open"].to_numpy(float), m["high"].to_numpy(float),
                    m["low"].to_numpy(float), m["close"].to_numpy(float),
                    pd.to_datetime(mts, unit="ms", utc=True))
        ei = {t: j for j, t in enumerate(ets)}
        in_b = np.array([t in bi for t in mts])
        in_e = np.array([t in ei for t in mts])
        L["L1"] += int(in_b.sum())
        okc = m["coverage_state"].to_numpy() == "COMPLETE"
        l2 = in_b & in_e & okc
        L["L2"] += int(l2.sum())
        pc = np.zeros(len(mts), bool); pc[1:] = (in_b & in_e)[:-1]
        po = np.zeros(len(mts), bool); po[1:] = okc[:-1]
        l3 = l2 & pc & po
        L["L3"] += int(l3.sum())
        if not l3.any():
            continue
        aj = np.array([ei[t] for t in mts[l3]]); bj = np.array([bi[t] for t in mts[l3]])
        a = A_all[aj]; bb = B_all[bj]; c = C_all[l3]
        AC_ok += int((a == c).sum()); BC_ok += int((bb == c).sum())
        if not np.array_equal(a != c, bb != c):
            row_identical = False
        for x, y in zip(a[a != c], c[a != c]):
            kk = f"{x}->{y}"; conf_AC[kk] = conf_AC.get(kk, 0) + 1
        for x, y in zip(bb[bb != c], c[bb != c]):
            kk = f"{x}->{y}"; conf_BC[kk] = conf_BC.get(kk, 0) + 1
        blank = a == "NO_T_LABEL"
        for v in c[blank]:
            raw_blank_vs_C[str(v)] = raw_blank_vs_C.get(str(v), 0) + 1

        ts = pd.to_datetime(mts[l3], unit="ms", utc=True).tz_convert(ET)
        et = ts.strftime("%H:%M"); day = ts.strftime("%Y-%m-%d")
        ii = m["interval_index"].to_numpy()[l3]; last = ii == ii.max()
        pv = np.array([MPOS.get(x, "OTHER") for x in et], dtype=object)
        pv[last & (pv == "OTHER")] = "SESSION_FINAL"
        for pn in pos_names:
            sel = pv == pn
            if sel.any():
                pos[pn]["n"] += int(sel.sum())
                pos[pn]["ac"] += int((a[sel] == c[sel]).sum())
                pos[pn]["bc"] += int((bb[sel] == c[sel]).sum())
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
            q = pair[pname]
            q["comparable"] += len(w); q["both_same"] += int((f1 & f2).sum())
            q["both_differ"] += int((~f1 & ~f2).sum())
            q["first_only"] += int((~f1 & f2).sum()); q["second_only"] += int((f1 & ~f2).sum())
        if n_i % 80 == 0:
            print(f"    {n_i+1}/{len(want)} · L0 {L['L0']:,} · L3 {L['L3']:,} · "
                  f"{time.time()-t0:.0f}s", flush=True)
    ce.close(); cb.close()

    if L["L3"] == 0:
        print("HOLD — zero L3 support"); return 1
    qualified_support = L["L0"] - ab_excluded
    exact_identity = ab_uncovered == 0
    if ab_uncovered:
        print(f"HOLD — {ab_uncovered} A!=B mismatch(es) have NO sealed boundary authority; "
              f"an exception may not be granted by resemblance")
    r = lambda n, d: round(n / d, 6) if d else None                     # noqa: E731
    status = "THREE_WAY_V2_REQUALIFIED_PASS" if (exact_identity and row_identical) else "HOLD"

    p = dict(
        report_id="MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2_REQUALIFIED",
        status=status,
        task_class="X_ONLY_CONFORMANCE_DIAGNOSTIC",
        supersedes=dict(artifact="MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V1",
                        digest="715765f2be4b8711",
                        why="V1 fed B the enriched store's own OHLC; the true input surface is "
                            "the lean base",
                        v1_A_vs_B_reclassified="WRONG_INPUT_SURFACE_DIAGNOSTIC — not a "
                                               "materializer-conformance residual",
                        not_edited=True),
        objects=dict(A="studio_15m.duckdb :: bars.t_sig",
                     B="canonical engine(studio_15m_base OHLC) — TRUE historical input",
                     C="canonical engine(Massive 15m OHLC) — T_MASSIVE_PORT"),

        supports=dict(L0_legacy_materializer_support=L["L0"],
                      L1_timestamp_common_base_massive=L["L1"],
                      L2_current_usable_all_sides=L["L2"],
                      L3_current_and_predecessor_aligned=L["L3"],
                      binding=dict(A_vs_B="L0", B_vs_C="L3", A_vs_C="L3 (identical rows)")),

        denominators=dict(
            RAW_LEGACY_MATERIALIZER_SUPPORT=L["L0"],
            CONFIRMED_BOUNDARY_ANOMALY=ab_excluded,
            QUALIFIED_LEGACY_MATERIALIZER_SUPPORT=qualified_support,
            reconciles=(L["L0"] - ab_excluded) == qualified_support),
        exclusion_guard=dict(
            basis="a row leaves the denominator ONLY if bound to a sealed "
                  "CONFIRMED_MATERIALIZATION_BOUNDARY_ANOMALY authority",
            authority=BOUND_D,
            covered_timestamp=COVERED_TS, covered_tickers=sorted(COVERED_TK),
            excluded=ab_excluded,
            mismatches_without_authority=ab_uncovered,
            negative_control="any A!=B mismatch without sealed authority is a HOLD",
            not_a_blacklist=True,
            scope="LEGACY MATERIALIZATION defect only — these rows are NOT removed from the "
                  "Massive research population, and B<->C never reads stored A"),
        invariant_A_equals_B=dict(
            requirement="EXACT on qualified support — an uncovered mismatch is a HOLD",
            raw_support=L["L0"], raw_mismatches=ab_mismatch,
            qualified_support=qualified_support,
            qualified_mismatches=ab_uncovered,
            holds=exact_identity,
            semantic_materializer_conformance=("EXACT" if exact_identity else "BREACHED"),
            examples=ab_examples,
            tolerance_forbidden=True,
            meaning="the canonical engine on the true historical input surface reproduces the "
                    "historical materialized label surface"),
        invariant_AC_equals_BC=dict(
            requirement="A<->C and B<->C must disagree on exactly the SAME rows, because A == B",
            row_level_identical=row_identical,
            A_vs_C_agreement=r(AC_ok, L["L3"]), B_vs_C_agreement=r(BC_ok, L["L3"]),
            confusion_identical=conf_AC == conf_BC,
            meaning_if_false="a support-alignment defect in the harness, not a finding about "
                             "the data"),

        source_port_divergence=dict(
            support=L["L3"], agreement=r(BC_ok, L["L3"]),
            divergence=round(1 - (BC_ok / L["L3"]), 6) if L["L3"] else None,
            top_disagreements=dict(sorted(conf_BC.items(), key=lambda kv: -kv[1])[:15]),
            interpretation="pure source/input divergence under identical canonical semantics"),

        blank_semantics=dict(
            legacy_blank_means="NO_MATERIALIZED_T_LABEL",
            legacy_blank_does_NOT_mean="EVALUABLE_NO_T_STATE",
            unchanged_by_provenance_resolution=True,
            why="knowing who wrote the column does not add an availability field to it",
            raw_what_C_says_on_blank_rows=dict(
                sorted(raw_blank_vs_C.items(), key=lambda kv: -kv[1])[:8]),
            allowed_claim="materialized T-token agreement",
            forbidden_claim="availability-semantic equivalence"),

        by_position={k: dict(bars=v["n"], A_vs_C=r(v["ac"], v["n"]), B_vs_C=r(v["bc"], v["n"]))
                     for k, v in pos.items()},
        registered_position_pairs={k: dict(**v, pair_portability=r(v["both_same"],
                                                                   v["comparable"]))
                                   for k, v in pair.items()},

        provenance_chain=[
            "update_all.sh -> derive_intraday.py --tf 15m",
            "studio_15m_base.duckdb (LEAN base); 15m resample = IDENTITY",
            "build_intraday_db._process",
            "main.api_bar_signals(..., _df=df)",
            "compute_all_signals -> signal_engine.compute_signals",
            "studio_15m.duckdb :: bars.t_sig"],
        authority_statement=("same canonical legacy 15m materializer semantics, verified "
                             "exactly against the historical materialized 15m signal surface "
                             "on its true input surface"),
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        elapsed_sec=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2_REQUALIFIED.json",
                 required=("report_id", "status", "objects", "supports",
                           "invariant_A_equals_B", "invariant_AC_equals_BC",
                           "source_port_divergence", "blank_semantics"),
                 supersede=os.path.exists(
                     "MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2_REQUALIFIED.json"))
    print(f"\nMASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2_REQUALIFIED · {d} · {status}")
    print(f"  supports    L0 {L['L0']:,} · L1 {L['L1']:,} · L2 {L['L2']:,} · L3 {L['L3']:,}")
    print(f"  denominators RAW {L['L0']:,} · anomaly {ab_excluded} · QUALIFIED "
          f"{qualified_support:,}")
    print(f"  A==B        qualified mismatches {ab_uncovered} -> EXACT {exact_identity} "
          f"(raw {ab_mismatch}, all covered by {BOUND_D})")
    print(f"  A<->C == B<->C rows identical: {row_identical} · confusion identical: "
          f"{conf_AC == conf_BC}")
    print(f"  source-port divergence {p['source_port_divergence']['divergence']} "
          f"(agreement {p['source_port_divergence']['agreement']})")
    print("  by position (bars · A<->C · B<->C):")
    for k, v in p["by_position"].items():
        print(f"    {k:14s} {v['bars']:>9,}  {v['A_vs_C']}  {v['B_vs_C']}")
    print("  pairs:")
    for k, v in p["registered_position_pairs"].items():
        print(f"    {k}  comparable {v['comparable']:,} · portable {v['pair_portability']}")
    return 0 if status.endswith("PASS") else 1


if __name__ == "__main__":
    raise SystemExit(main())
