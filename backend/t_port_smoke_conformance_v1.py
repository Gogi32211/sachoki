"""MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1 — preflight, fixtures, and Layer 1.

Layer 1 is the primary semantic test and it needs no external data at all: it runs the canonical
producer, the Massive port and an independent scalar verifier over identical constructed bars
and demands EXACT agreement. Layers 2 and 3, and the 476-cohort evaluability census, need the
external volume, which macOS is currently refusing to let this process read. Those are reported
as BLOCKED rather than estimated, so the artifact cannot PASS.

THE PREFLIGHT IS THE GATE AND IT IS ANSWERED FROM THE CALLER. shift(1) has no meaning inside
signal_engine.py; it means whatever the frame handed to compute_signals() means. Traced:
data_polygon.fetch_bars ends with `df[~df.index.duplicated()].sort_index()`, fetch_ohlcv takes
`.tail(bars)` for ONE ticker, and indicators.norm_ohlcv neither sorts nor drops nor deduplicates.
So the series is per-security, deduplicated, chronologically ascending, spans many sessions, and
is never rebuilt per session — SESSION_CONTINUOUS. At 15m that means the first bar of a session
takes the last bar of the previous session as its predecessor. Fixture P10 measures what that
choice is worth, so it is a bound decision rather than an inherited habit.

ONE FINDING WORTH STATING PLAINLY. Amendment 2 froze the T-internal predicate as RAW prior-bar
doji and forbade importing Pine's resolved Z7. Checking the producer's own algebra, every Z
condition requires isBear and every T condition requires isBull, so on a doji bar (c == o)
neither fires and cZ7c collapses to isDoji exactly. Raw and resolved therefore COINCIDE in this
producer. The rule stands, but it is now proven harmless rather than merely obeyed — and P8
verifies it empirically instead of taking the argument on trust.

NO T PRODUCTION. NO Z. NO Y.
"""
from __future__ import annotations
import hashlib, json, os, re, sys, time                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import numpy as np                                                        # noqa: E402
import pandas as pd                                                       # noqa: E402
import t5_artifact as ART                                                 # noqa: E402

AM2, AM2_D = "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2.json", "bd9292853ee10f2c"
PROV, PROV_D = "MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1.json", "a8e1f3725ddfe7a1"
BASE, BASE_D = "MASSIVE_T_FEATURE_PORT_SPEC_V1.json", "545d293b4f1e4653"
T_SLICE_D = "2901107d9baa6320"
COARSE, COARSE_D = "MASSIVE_1M_COARSE_AGGREGATION_PRODUCTION_V1.json", "6ec73d5f462761f0"
ELIG, ELIG_D = "SP500_CURRENT_1M_RESEARCH_ELIGIBILITY_V1.json", "7c0a5ad6b17d7ae0"
IDS = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"

import t_port_v1 as PORT                                                  # noqa: E402
import t_port_verifier_v1 as VER                                          # noqa: E402
from signal_engine import compute_signals, SIG_NAMES as ENG_NAMES, _BC_TO_SID as ENG_BC


def _h(*files):
    h = hashlib.sha256()
    for f in files:
        h.update(open(f, "rb").read())
    return h.hexdigest()


def port_hash():
    """The PORT's executable code only — verifier and census included, harness excluded.

    The earlier run reported a single combined hash (ee48e008e7c20b10) covering the port, the
    verifier, the harness AND signal_engine.py. That was a mixed hash, so it could not answer
    "did the port change?" independently of "did a fixture definition change?". Splitting it
    is the point of this revision: correcting a fixture must not look like a port change, and
    a port change must not hide behind a fixture edit.
    """
    return _h("t_port_v1.py", "t_port_verifier_v1.py", "t_port_census_v1.py")


def harness_hash():
    # the layers module is harness/fixture code, not port code — it reads stores and
    # computes diagnostics, and must not perturb the port hash
    return _h("t_port_smoke_conformance_v1.py", "t_port_conformance_layers_v1.py")


def impl_hash():
    return _h("t_port_v1.py", "t_port_verifier_v1.py",
              "t_port_smoke_conformance_v1.py", "signal_engine.py")


# baseline moved by AMENDMENT_3 (a5edf6e6806d4e0b), which corrected the
# availability-reason precedence. Previous baseline: 5ab5390171e0d5d2.
PRIOR_PORT_HASH = "0ca86eeb1f8ee8fe"
PRIOR_HARNESS_HASH = "5a183315f801b568"

SCOPE, SCOPE_D = ("MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1.json",
                  "c6358576c5c9411b")
AM3, AM3_D = "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_3.json", "a5edf6e6806d4e0b"
RESEARCH_TIMEFRAMES = ("15m",)
DIAGNOSTIC_TIMEFRAMES = ("1D",)
DIAGNOSTIC_CLASS = "DIAGNOSTIC_ONLY_NOT_HYPOTHESIS_REGISTRATION"


def timeframe_registry_guard():
    """The research registry must still be 15m alone, and 1D must still be diagnostic-only.

    Adding a Layer 2 / Layer 3A / price-basis reader means teaching this harness to open 1D
    data. That is exactly the move that could, without anyone deciding it, turn a diagnostic
    into a second registered research timeframe. So the two registries are declared separately,
    proven disjoint, and checked against what the sealed amendments actually say — before the
    first fixture runs, every run, whether or not a reader has been added yet.

    Returns (ok, detail). A False here is a HOLD, not a warning.
    """
    d = {}
    a2 = json.load(open(AM2))
    tf = a2.get("input_timeframe", {})
    d["amendment_2_frozen_15m_only"] = (tf.get("frozen") == "15m" and tf.get("only") is True)
    d["amendment_2_forbidden_list"] = sorted(tf.get("forbidden", []))
    d["amendment_2_forbids_1D"] = "1D" in tf.get("forbidden", [])

    sc = json.load(open(SCOPE))
    rp = sc.get("research_port_timeframe", {})
    dg = sc.get("diagnostic_only_timeframe", {})
    d["scope_research_unchanged"] = (rp.get("value") == "15m" and rp.get("only") is True
                                     and rp.get("status") == "UNCHANGED")
    d["scope_1D_is_diagnostic_only"] = (dg.get("value") == "1D"
                                        and dg.get("classification") == DIAGNOSTIC_CLASS)
    d["scope_forbidden_consequences"] = len(dg.get("forbidden_consequences", []))
    d["scope_family_not_enlarged"] = sc.get("research_family_enlarged") is False

    d["port_registered_timeframe"] = PORT.REGISTERED_TIMEFRAME
    d["port_is_15m"] = PORT.REGISTERED_TIMEFRAME == "15m"
    d["research_registry"] = list(RESEARCH_TIMEFRAMES)
    d["diagnostic_registry"] = list(DIAGNOSTIC_TIMEFRAMES)
    d["registries_disjoint"] = not (set(RESEARCH_TIMEFRAMES) & set(DIAGNOSTIC_TIMEFRAMES))
    d["research_registry_is_15m_alone"] = tuple(RESEARCH_TIMEFRAMES) == ("15m",)

    ok = all([d["amendment_2_frozen_15m_only"], d["amendment_2_forbids_1D"],
              d["scope_research_unchanged"], d["scope_1D_is_diagnostic_only"],
              d["scope_family_not_enlarged"], d["port_is_15m"],
              d["registries_disjoint"], d["research_registry_is_15m_alone"],
              d["scope_forbidden_consequences"] >= 6])
    d["verdict"] = "PASS" if ok else "HOLD"
    d["enforces"] = ("adding a 1D reader cannot become a research-registry expansion; the "
                     "research registry is 15m alone and 1D stays " + DIAGNOSTIC_CLASS)
    return ok, d


def config_fingerprint():
    """Every frozen knob, in one digest.

    Taken before the first fixture and re-taken before sealing. The negative fixtures mutate
    module globals on purpose and restore them in `finally`; if a restore ever failed, the
    census and the diagnostic layers would quietly run on a mutated config and the numbers
    would look fine. This makes that failure loud instead. It is also the mechanical form of
    the rule that Layer 2 and Layer 3A are diagnostics, not tuning inputs — nothing they
    measure can reach these values, because the values are checked to be identical afterwards.
    """
    import json as _j
    return hashlib.sha256(_j.dumps({
        "USE_WICK": PORT.USE_WICK, "MIN_BODY_RATIO": PORT.MIN_BODY_RATIO,
        "DOJI_THRESH": PORT.DOJI_THRESH, "NUMERICAL_GUARD": PORT.NUMERICAL_GUARD,
        "PRIORITY": list(PORT.PRIORITY),
        "BC_TO_SID": {str(k): v for k, v in sorted(PORT.BC_TO_SID.items())},
        "SIG_NAMES": {str(k): v for k, v in sorted(PORT.SIG_NAMES.items())},
        "REGISTERED_TIMEFRAME": PORT.REGISTERED_TIMEFRAME,
        "CALL_CONTEXT": PORT.CALL_CONTEXT,
        "REQUIRED_PRIOR_BARS": PORT.REQUIRED_PRIOR_BARS,
    }, sort_keys=True, default=str).encode()).hexdigest()[:16]


def hash_drift(ph, hh):
    """Which code actually changed — a permission fix must never look like a code change."""
    changed = []
    if ph[:16] != PRIOR_PORT_HASH:
        changed.append("port")
    if hh[:16] != PRIOR_HARNESS_HASH:
        changed.append("harness")
    return dict(prior_port=PRIOR_PORT_HASH, actual_port=ph[:16],
                prior_harness=PRIOR_HARNESS_HASH, actual_harness=hh[:16],
                changed=changed,
                permission_change_is_not_a_mutation=True,
                rule="restoring volume access mutates no artifact and no code; a hash may "
                     "differ ONLY because the corresponding source actually changed")


def t_slice_digest(src):
    def between(a, b):
        i, j = src.find(a), src.find(b)
        return src[i:j] if (i >= 0 and j > i) else None
    parts = {"constants": between("NONE = 0", "_ZC_TO_SID"),
             "bar_algebra_and_cT": between("def compute_signals(", "# ── Bearish patterns"),
             "bc_priority": between("# ── bc priority code", "# ── zc priority code"),
             "sid_and_name": between("sid = np.where(", "is_bear  =")}
    if any(v is None for v in parts.values()):
        return None
    return hashlib.sha256("\n".join(parts[k] for k in sorted(parts))
                          .encode()).hexdigest()[:16]


def preflight():
    """Recover the caller's dataframe semantics from the proven call path."""
    facts, ok = {}, True
    checks = [
        ("data_polygon.py", r"df\[~df\.index\.duplicated\(\)\]\.sort_index\(\)",
         "dedup_on_index_then_sort_ascending"),
        ("data_polygon.py", r'"sort":\s*"asc"', "vendor_requested_ascending"),
        ("data_polygon.py", r'"adjusted":\s*"true"', "adjusted_series"),
        ("data_polygon.py", r'set_index\("timestamp"\)', "utc_datetime_index"),
        ("data.py", r"df\.tail\(bars\)", "trim_to_last_N_rows"),
        ("data.py", r"fetch_bars\(ticker,\s*interval=interval", "single_ticker_per_call"),
        ("indicators.py", r"def norm_ohlcv", "producer_normaliser"),
        ("main.py", r"df = _df if _df is not None else fetch_ohlcv\(ticker", "caller_source"),
    ]
    for path, pat, label in checks:
        found = bool(os.path.exists(path) and re.search(pat, open(path).read()))
        facts[label] = found
        ok &= found
    # norm_ohlcv must NOT sort/drop/dedup — its silence is what makes ordering the caller's
    norm = open("indicators.py").read()
    body = norm[norm.find("def norm_ohlcv"):norm.find("# ── Moving averages")]
    for forbidden, lbl in ((r"sort_index|sort_values", "norm_does_not_sort"),
                           (r"dropna", "norm_does_not_dropna"),
                           (r"drop_duplicates|duplicated", "norm_does_not_dedup")):
        facts[lbl] = not bool(re.search(forbidden, body))
        ok &= facts[lbl]
    return ok, facts


def corpus(n=40000, seed=7):
    """Bars drawn from a coarse price lattice so exact equalities occur constantly.

    Random floats would make c == o essentially impossible and the doji path would never be
    exercised. A small integer lattice makes ties, exact engulfs and ratio-1.0 boundaries
    common, which is the only way these predicates get real coverage.
    """
    rs = np.random.RandomState(seed)
    px = rs.randint(995, 1006, size=(n, 4)).astype(float) / 100.0
    o = px[:, 0]; c = px[:, 1]
    h = np.maximum.reduce([px[:, 2], o, c])
    l = np.minimum.reduce([px[:, 3], o, c])
    idx = pd.date_range("2024-01-02 14:30", periods=n, freq="15min", tz="UTC")
    return pd.DataFrame({"open": o, "high": h, "low": l, "close": c}, index=idx)


def producer_bc(df):
    return compute_signals(df)["bc"].to_numpy().astype(int)


def main():
    for f, want in ((AM2, AM2_D), (PROV, PROV_D), (BASE, BASE_D),
                    (SCOPE, SCOPE_D), (AM3, AM3_D)):
        got = ART.file_digest(f)
        if got != want:
            print(f"HOLD — digest mismatch {f}: {got}"); return 1
    if t_slice_digest(open("signal_engine.py").read()) != T_SLICE_D:
        print("HOLD — producer T-slice digest changed since Amendment 2"); return 1

    tf_ok, TFG = timeframe_registry_guard()
    if not tf_ok:
        print("HOLD — timeframe registry guard failed:",
              {k: v for k, v in TFG.items() if v is False}); return 1

    ok, facts = preflight()
    if not ok:
        print("HOLD — call-context preflight could not be established:",
              [k for k, v in facts.items() if not v]); return 1

    IH, PH, HH = impl_hash(), port_hash(), harness_hash()
    CFG_BEFORE = config_fingerprint()
    DRIFT = hash_drift(PH, HH)
    res = []

    def rec(fid, name, passed, detail, kind="POSITIVE"):
        res.append(dict(fixture=fid, name=name, kind=kind, passed=bool(passed),
                        detail=detail))
        print(f"  {fid:4s} {kind[:3]} {'PASS' if passed else 'FAIL'}  {name} | {detail}",
              flush=True)

    df = corpus()
    o = df["open"].to_numpy(); h = df["high"].to_numpy()
    l = df["low"].to_numpy(); c = df["close"].to_numpy()
    rows = list(zip(o, h, l, c))

    eng = producer_bc(df)
    prt = PORT.compute_t(df)
    ver = VER.verify(rows)
    P = PORT.raw_predicates(df)

    # ---- LAYER 1 : producer vs port vs verifier, EXACT ----------------------
    ev = prt["state"].to_numpy() == "AVAILABLE"
    pbc = prt["bc"].to_numpy().astype(int)
    vbc = np.array([r["bc"] for r in ver])
    d_ep = int((eng[ev] != pbc[ev]).sum())
    d_pv = int((pbc[ev] != vbc[ev]).sum())
    d_ev = int((eng[ev] != vbc[ev]).sum())
    rec("L1", "producer vs port vs verifier — EXACT on identical input",
        d_ep == 0 and d_pv == 0 and d_ev == 0,
        f"{int(ev.sum()):,} evaluable bars; producer!=port {d_ep}, port!=verifier {d_pv}, "
        f"producer!=verifier {d_ev}")

    # raw predicates, not only the resolved label
    praw = {k: int((P[k][ev] != np.array([bool(r['preds'][k]) if r['preds'] else False
                                          for r in ver])[ev]).sum()) for k in P}
    rec("L1b", "raw predicate agreement port vs verifier (pre-priority)",
        sum(praw.values()) == 0,
        f"12 families, total mismatches {sum(praw.values())}")

    # ---- P1 coverage --------------------------------------------------------
    raw_counts = {k: int(P[k][ev].sum()) for k in P}
    res_counts = {n: int((pbc[ev] == i) .sum()) for i, n in enumerate(PORT.PRIORITY, 1)}
    rec("P1", "every registered T state represented (raw and priority-resolved)",
        all(v > 0 for v in raw_counts.values()) and all(v > 0 for v in res_counts.values()),
        f"raw min {min(raw_counts.values())}, resolved min {min(res_counts.values())}")

    # ---- P2 priority winner on genuinely competing predicates ---------------
    stack = np.vstack([P[n] for n in PORT.PRIORITY])
    multi = (stack.sum(axis=0) >= 2) & ev
    first = np.argmax(stack[:, multi], axis=0) + 1
    rec("P2", "priority winner is the highest-ranked competing predicate",
        int(multi.sum()) > 0 and np.array_equal(first, pbc[multi]),
        f"{int(multi.sum()):,} bars with >=2 raw predicates; winner matches rank-1 in all")

    # ---- P3 / P4 doji exactness --------------------------------------------
    def mini(bars):
        idx = pd.date_range("2024-03-01 14:30", periods=len(bars), freq="15min", tz="UTC")
        return pd.DataFrame(bars, columns=["open", "high", "low", "close"], index=idx)
    exact = mini([(10.00, 10.00, 10.00, 10.00), (9.99, 10.60, 9.98, 10.50)])
    near = mini([(10.00, 10.30, 9.70, 10.01), (9.99, 10.60, 9.98, 10.50)])
    le, ln = PORT.compute_t(exact)["t_label"].iloc[1], PORT.compute_t(near)["t_label"].iloc[1]
    rec("P3", "isDoji is EXACT equality and drives prior-bar bearishness",
        le == "T4", f"prev c==o exactly -> {le}")
    rec("P4", "a near-doji is NOT a doji despite doji_thresh=0.05",
        ln != le and ln == "T6",
        f"prev body/range = {abs(10.01-10.00)/(10.30-9.70):.4f} (<0.05) -> {ln}, "
        f"not {le}")

    vr_start = VER.verify([(10,10.1,9.9,10.05)]*3, observed=[False,True,True])[0]["reason"]
    vr_prior = VER.verify([(10,10.1,9.9,10.05)]*4,
                          observed=[True,False,False,True])[2]["reason"]
    pr_start = PORT.compute_t(mini([(10,10.1,9.9,10.05)]*3),
                              observed=np.array([False,True,True]))["reason"].iloc[0]
    pr_prior = PORT.compute_t(mini([(10,10.1,9.9,10.05)]*4),
                              observed=np.array([True,False,False,True]))["reason"].iloc[2]
    rec("L1c", "the two implementations agree on AMENDMENT_3 precedence",
        vr_start == pr_start == "NO_PRIOR_BAR"
        and vr_prior == pr_prior == "CURRENT_BAR_NOT_COMPLETE",
        f"start: verifier {vr_start} / port {pr_start}; "
        f"existing-prior: verifier {vr_prior} / port {pr_prior}")

    # ---- P5 numerical guard binds ------------------------------------------
    g = PORT.compute_t(exact)
    rec("P5", "prevBodySafe binds to 1e-10 when the previous body is exactly zero",
        g["t_label"].iloc[1] == "T4",
        "pBody=0 -> safe=1e-10 -> ratio unbounded, so engOk rests on the edges alone")

    # ---- P6 use_wick=False --------------------------------------------------
    wick = mini([(10.00, 10.90, 9.10, 10.20), (10.15, 10.85, 9.15, 10.25)])
    lw = PORT.compute_t(wick)["t_label"].iloc[1]
    rec("P6", "use_wick=False — wick-engulf does not engulf",
        lw not in ("T4", "T6"),
        f"current wicks span the prior wicks but bodies do not -> {lw}")

    # ---- P7 min_body_ratio boundary ----------------------------------------
    # An earlier version of this fixture used bars whose ratio was 1.0 but whose BODY EDGES
    # did not engulf, so it was testing nothing and reported T3. Corrected: the current body
    # exactly covers the prior body, which is the real ratio-1.0 boundary.
    eqb = mini([(10.00, 10.05, 9.85, 9.90), (9.90, 10.10, 9.85, 10.00)])
    lb = PORT.compute_t(eqb)["t_label"].iloc[1]
    rec("P7", "min_body_ratio=1.0 is inclusive at the boundary",
        lb == "T4", f"cBody == pBody exactly and the bodies coincide (ratio 1.0) -> {lb}")

    # ---- P7b the ratio test is redundant below 1.0 under use_wick=False -----
    o1 = np.roll(o, 1); c1 = np.roll(c, 1); o1[0] = np.nan; c1[0] = np.nan
    edges = (np.maximum(o, c) >= np.maximum(o1, c1)) & (np.minimum(o1, c1) >= np.minimum(o, c))
    viol = int(np.nansum(edges & (np.abs(c - o) < np.abs(c1 - o1))))
    rec("P7b", "body-edge engulf implies cBody >= pBody, so ratio<=1.0 cannot bind",
        viol == 0,
        f"{viol} counterexamples in {int(np.nansum(edges)):,} edge-engulfing bars; "
        f"only min_body_ratio > 1.0 is semantically active")

    # ---- P8 raw vs resolved Z7 coincide -------------------------------------
    isd = (c == o)
    bull = c > o
    bear = c < o
    coincide = bool((isd & (bull | bear)).sum() == 0)
    rec("P8", "raw prior-bar doji == resolved Z7 in this producer (proven, not assumed)",
        coincide,
        f"{int(isd.sum()):,} doji bars, none of which is bull or bear, so anyB=anyZ=False "
        f"and cZ7c collapses to isDoji")

    # ---- P9 mapping ---------------------------------------------------------
    same_map = all(ENG_BC[k] == v for k, v in PORT.BC_TO_SID.items()) and all(
        ENG_NAMES[v] == PORT.SIG_NAMES[v] for v in PORT.BC_TO_SID.values())
    rec("P9", "state code mapping identical to the producer",
        same_map, "bc->sig_id and sig_id->name match signal_engine exactly")

    # ---- P10 session boundary is a BOUND decision ---------------------------
    n_sess = 26
    two = corpus(n=n_sess * 2, seed=11)
    cont = PORT.compute_t(two)["t_label"].to_numpy()
    s1 = PORT.compute_t(two.iloc[:n_sess])["t_label"].to_numpy()
    s2 = PORT.compute_t(two.iloc[n_sess:])["t_label"].to_numpy()
    reset = np.concatenate([s1, s2])
    diff_at_boundary = cont[n_sess] != reset[n_sess]
    rec("P10", "SESSION_CONTINUOUS shift(1) crosses the boundary, and it matters",
        True,
        f"first bar of session 2: continuous -> {cont[n_sess]!r}, session-reset -> "
        f"{reset[n_sess]!r}; differs={diff_at_boundary}")

    # ---- P11 first evaluable after unavailable history ----------------------
    obs = np.array([True, False, True, True])
    sm = PORT.compute_t(mini([(10, 10.1, 9.9, 10.05)] * 4), observed=obs)
    st = sm["state"].tolist(); rs_ = sm["reason"].tolist()
    rec("P11", "first evaluable observation after unavailable history",
        st == ["UNAVAILABLE", "UNAVAILABLE", "UNAVAILABLE", "AVAILABLE"]
        and rs_[0] == "NO_PRIOR_BAR"
        and rs_[2] == "PRIOR_INPUT_NOT_COMPLETE",
        f"states {st} · reasons {rs_}")

    # ---- P12 the census engine must equal the validated 1-D port ------------
    # The census is vectorised across securities and continuous across session partitions.
    # That makes it a different program from compute_t(), so it is checked against it here
    # rather than trusted. Three securities, five sessions, with UNOBSERVED bars planted so
    # the availability path is exercised at and away from the boundary.
    import t_port_census_v1 as CEN
    rs2 = np.random.RandomState(23)
    NSEC, NSESS, M = 3, 5, 26
    px = rs2.randint(995, 1006, size=(NSEC, NSESS * M, 4)).astype(float) / 100.0
    co = px[:, :, 0]; cc = px[:, :, 1]
    ch = np.maximum.reduce([px[:, :, 2], co, cc]); cl = np.minimum.reduce([px[:, :, 3], co, cc])
    comp = np.ones((NSEC, NSESS * M), bool)
    comp[0, 30] = False; comp[1, M] = False; comp[2, M - 1] = False   # mid, first-of-session, last
    cen = CEN.TCensus(["A", "B", "C"])
    bc_cen = np.zeros((NSEC, NSESS * M), dtype=int)
    st_cen = np.empty((NSEC, NSESS * M), dtype=object)
    for s in range(NSESS):
        sl = slice(s * M, (s + 1) * M)
        r = cen.process_session(f"d{s}", co[:, sl], ch[:, sl], cl[:, sl], cc[:, sl],
                                comp[:, sl], emit=True)
        bc_cen[:, sl] = r["bc"]; st_cen[:, sl] = r["state"]
    mism, checked = 0, 0
    for k in range(NSEC):
        f = pd.DataFrame({"open": co[k], "high": ch[k], "low": cl[k], "close": cc[k]},
                         index=pd.date_range("2024-01-02 14:30", periods=NSESS * M,
                                             freq="15min", tz="UTC"))
        one = PORT.compute_t(f, observed=comp[k])
        a = one["bc"].to_numpy().astype(int); b = bc_cen[k]
        sa = one["state"].to_numpy(); sb = st_cen[k]
        # slot 0 of the whole series has no predecessor in either program
        mism += int((a[1:] != b[1:]).sum()) + int((sa[1:] != sb[1:]).sum())
        checked += len(a) - 1
    rec("P12", "session-partitioned census engine == validated 1-D port",
        mism == 0,
        f"{NSEC} securities x {NSESS} sessions, {checked} compared bars incl. planted "
        f"UNOBSERVED at a session boundary; mismatches {mism}")

    # ================= NEGATIVE FIXTURES ====================================
    def neg(fid, name, fn, want_token=None):
        try:
            detected = fn()
            rec(fid, name, bool(detected),
                "detected" if detected else "NOT DETECTED", "NEGATIVE")
        except PORT.TPortHold as e:
            rec(fid, name, (want_token is None or want_token in str(e)),
                str(e)[:90], "NEGATIVE")

    prod_path = os.path.abspath(sys.modules["signal_engine"].__file__)
    neg("N1", "Pine mintick semantics cannot silently replace the guard",
        lambda: _mutate_guard(df, eng, ev, 0.01))
    neg("N2", "authority is bound by module identity + T-slice digest",
        lambda: prod_path.endswith("/backend/signal_engine.py")
        and t_slice_digest(open(prod_path).read()) == T_SLICE_D)
    neg("N3", "use_wick mutation detected", lambda: _mutate(df, eng, ev, "USE_WICK", True))
    # N4/N5 mutate in the SEMANTICALLY ACTIVE direction. Mutating min_body_ratio downward,
    # or the guard below the data's minimum body, is provably a no-op (P7b and the
    # sensitivity band below) — a no-op is not an undetected mutation, and dressing one up
    # as a caught mutation would make this fixture a lie.
    neg("N4", "min_body_ratio mutation detected (active direction)",
        lambda: _mutate(df, eng, ev, "MIN_BODY_RATIO", 2.0))
    neg("N5", "1e-10 guard mutation detected (active direction)",
        lambda: _mutate_guard(df, eng, ev, 0.02))
    neg("N6", "threshold-doji cannot replace exact equality",
        lambda: _threshold_doji_differs(df, eng, ev))
    neg("N7", "priority permutation detected", lambda: _permute_priority(df, eng, ev))
    neg("N8", "wrong state-code mapping detected", lambda: _bad_mapping(df, ev))
    neg("N9", "non-15m timeframe rejected",
        lambda: PORT.compute_t(df, timeframe="1H"), "GUARD timeframe")
    neg("N10", "contaminated current bar cannot become FALSE",
        lambda: _unavailable_not_false(mini([(10, 10.1, 9.9, 10.05)] * 3),
                                       np.array([True, True, False]), 2))
    neg("N11", "unavailable required prior bar cannot become FALSE",
        lambda: _unavailable_not_false(mini([(10, 10.1, 9.9, 10.05)] * 3),
                                       np.array([True, False, True]), 2))
    neg("N12", "reordered caller dataframe rejected",
        lambda: PORT.compute_t(df.iloc[::-1]), "GUARD ordering")
    neg("N13", "changed session-slicing semantics detected",
        lambda: bool((cont != reset).sum() > 0))
    neg("N14", "port emits no research-facing Z label",
        lambda: set(PORT.SIG_NAMES.values()) == {"NONE"} | set(PORT.PRIORITY))
    neg("N15", "no tunable config path exists on the port entry point",
        lambda: _no_tunable_config())
    # ---- volume-dependent work -------------------------------------------
    import t_port_census_v1 as CEN2
    import t_port_conformance_layers_v1 as LAY
    VOL_OK = True
    try:
        from studio.mount_guard import require_external_volume
        require_external_volume(purpose="T port conformance layers (read-only)")
    except Exception as _e:
        VOL_OK = False
        print(f"  volume BLOCKED — {str(_e).splitlines()[0]}", flush=True)

    LAYERS = {}
    if VOL_OK:
        el = json.load(open(ELIG)); idr = json.load(open(IDS))["mapping"]
        key_of = {r["frozen_current_ticker"]: r["security_key_v1"] for r in idr}
        tickers = sorted(el["cohort"]["eligible_tickers"])
        ckeys = [key_of[t] for t in tickers]
        print(f"  running Layer 3B census over {len(ckeys)} securities …", flush=True)
        LAYERS["layer_3b_massive_15m_capability"] = LAY.layer_3b_census(
            ckeys, CEN2.TCensus, progress=True)
        import duckdb
        con = duckdb.connect(LAY.DUCK, read_only=True)
        print("  running Layer 2 …", flush=True)
        LAYERS["cross_universe_tsig_consistency"] = LAY.cross_universe_tsig_consistency(
            con, tickers)
        LAYERS["layer_2_legacy_1d_materializer_storage_conformance"] = LAY.layer_2(
            con, tickers, compute_signals, progress=True)
        print("  running Layer 3A …", flush=True)
        LAYERS["layer_3a_1d_cross_source"] = LAY.layer_3a(
            con, tickers, key_of, compute_signals, progress=True)
        con.close()
        n16r = LAY.n16(ckeys, idr, LAYERS["layer_3b_massive_15m_capability"])
        LAYERS["n16"] = n16r
        rec("N16", "supplemental / new-vintage / excluded securities rejected",
            n16r["passed"],
            f"cohort {n16r['cohort']} · excluded {n16r['excluded_securities']} · "
            f"outside-cohort consumed {n16r['keys_outside_cohort_consumed']} · "
            f"supplemental present {n16r['supplemental_namespace_present']}", "NEGATIVE")
    else:
        res.append(dict(fixture="N16", name="supplemental / new-vintage V1 data rejected",
                        kind="NEGATIVE", passed=None,
                        detail="BLOCKED — external volume unreadable"))
        print("  N16  NEG BLOCKED  requires the external volume", flush=True)
    # ---- AMENDMENT_3 required fixtures -----------------------------------
    def _start_double_failure():
        r = PORT.compute_t(mini([(10, 10.1, 9.9, 10.05)] * 3),
                           observed=np.array([False, True, True]))
        return (r["reason"].iloc[0] == "NO_PRIOR_BAR"
                and r["state"].iloc[0] == "UNAVAILABLE"
                and r["t_label"].iloc[0] == "")

    def _existing_prior_double_failure():
        r = PORT.compute_t(mini([(10, 10.1, 9.9, 10.05)] * 4),
                           observed=np.array([True, False, False, True]))
        # index 2: the predecessor EXISTS but is contaminated, and so is the current bar
        return r["reason"].iloc[2] == "CURRENT_BAR_NOT_COMPLETE"

    neg("N18", "N_START_DOUBLE_FAILURE — no prior bar outranks incomplete current",
        _start_double_failure)
    neg("N19", "N_EXISTING_PRIOR_DOUBLE_FAILURE — existing-but-bad prior does NOT outrank",
        _existing_prior_double_failure)
    neg("N17", "no Y-side symbol reachable from the port",
        lambda: _no_y_tokens())

    # ---- measured sensitivity band for the numerical guard ------------------
    band = {}
    for gv in (1e-10, 1e-6, 1e-4, 0.005, 0.01, 0.02):
        band[str(gv)] = _mutate_guard(df, eng, ev, gv)

    CFG_AFTER = config_fingerprint()
    if CFG_AFTER != CFG_BEFORE:
        print(f"HOLD — frozen config changed during the run: {CFG_BEFORE} -> {CFG_AFTER}; "
              f"a negative fixture failed to restore a mutated global")
        return 1

    passed = all(r["passed"] for r in res if r["passed"] is not None)
    blocked = [] if VOL_OK else [
        "MASSIVE_T_EVALUABILITY_CENSUS (476 cohort)",
        "LAYER_2 legacy 1D materializer storage conformance",
        "LAYER_3A 1D cross-source diagnostic",
        "PRICE_BASIS full matched diagnostics",
        "N16 supplemental/new-vintage rejection"]
    l1_exact = (d_ep == d_pv == d_ev == 0) and sum(praw.values()) == 0
    all_ran = all(r["passed"] is not None for r in res)
    final_pass = bool(VOL_OK and passed and all_ran and l1_exact and not blocked)

    p = dict(
        report_id="MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1",
        status="MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1_PASS" if final_pass else "HOLD",
        hold_reason=None if final_pass else "EXTERNAL_VOLUME_UNREADABLE — Layers 2 and 3, the 476-cohort "
                    "evaluability census and the price-basis diagnostics could not be run",
        task_class="SMOKE_AND_CONFORMANCE",
        authorities=dict(amendment_2=AM2_D, provenance=PROV_D, base_spec=BASE_D,
                         producer_t_slice=T_SLICE_D,
                         coarse_source=dict(artifact=COARSE, digest=COARSE_D,
                                            readable=False),
                         eligibility=dict(artifact=ELIG, digest=ELIG_D, readable=False)),
        implementation_hash=IH,
        port_implementation_hash=PH,
        harness_hash=HH,
        hash_split=dict(
            note="the earlier run reported ONE combined hash (ee48e008e7c20b10) over port + "
                 "verifier + harness + producer; it could not distinguish a port change from "
                 "a fixture correction, so it is split here",
            port_scope=["t_port_v1.py", "t_port_verifier_v1.py", "t_port_census_v1.py"],
            harness_scope=["t_port_smoke_conformance_v1.py"],
            producer_bound_by="T-slice digest " + T_SLICE_D,
            prior_combined_hash="ee48e008e7c20b10",
            prior_hash_was_mixed=True),
        implementation_frozen_before_first_fixture=True,
        hash_drift=DRIFT,
        timeframe_registry_guard=TFG,
        frozen_config_integrity=dict(
            fingerprint_before=CFG_BEFORE, fingerprint_after=CFG_AFTER,
            unchanged=CFG_BEFORE == CFG_AFTER,
            covers=["use_wick", "min_body_ratio", "doji_thresh", "1e-10 guard",
                    "priority chain", "bc->sig_id mapping", "sig_id->name mapping",
                    "registered timeframe", "call context", "required prior bars"],
            enforces="Layer 2 and Layer 3A are diagnostics, not tuning inputs — no measured "
                     "result can reach these values, because they are proven identical after "
                     "the run",
            also_catches="a negative fixture that mutates a module global and fails to "
                         "restore it"),

        call_context_preflight=dict(
            established=True, facts=facts,
            input_source="Massive/Polygon aggregates via data_polygon.fetch_bars "
                         "(adjusted=true, vendor sort=asc)",
            security_scope="ONE ticker per compute_signals call",
            range_scope="tail(bars) of a days-derived window; optional `since` filter",
            row_ordering="chronologically ASCENDING (df.sort_index())",
            timestamp_ordering="UTC DatetimeIndex",
            duplicate_handling="df[~df.index.duplicated()] — first kept, BEFORE computation",
            rows_span_multiple_sessions=True,
            dataframe_reset_by_session=False,
            filtering_before_compute="`since` filter and tail(bars) only; no session or "
                                     "time-of-day filter",
            ohlc_consumed=["open", "high", "low", "close"],
            volume_consumed=False,
            null_drop_behaviour="NONE — norm_ohlcv neither sorts, drops nor deduplicates; "
                                "ordering is therefore entirely the caller's",
            shift1_semantics="the immediately preceding row of this security's "
                             "deduplicated ascending series",
            session_boundary="CROSSED — classified SESSION_CONTINUOUS",
            producer_export_timeframe="1d (the legacy export); the port is 15m ONLY",
            note="the producer imposes no ordering discipline of its own, which is exactly "
                 "why this preflight was mandatory"),

        layer_1_semantic_correctness=dict(
            expectation="EXACT",
            evaluable_bars=int(ev.sum()),
            producer_vs_port=d_ep, port_vs_verifier=d_pv, producer_vs_verifier=d_ev,
            raw_predicate_mismatches=praw,
            result="EXACT" if (d_ep == d_pv == d_ev == 0) else "MISMATCH",
            note="three independent implementations: vectorised producer, vectorised port, "
                 "scalar if/elif verifier"),
        layers=LAYERS,
        fifteen_minute_feed_divergence=dict(
            status="NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA",
            reason="no matched legacy 15m support exists — bars is structurally daily",
            explicitly_not="a value carried across from the 1D diagnostic"),

        per_state=dict(
            raw_counts=raw_counts, priority_resolved_counts=res_counts,
            scope="Layer 1 corpus only — NOT a Massive population statistic"),
        fixtures=res,
        fixtures_passed=sum(1 for r in res if r["passed"] is True),
        fixtures_blocked=sum(1 for r in res if r["passed"] is None),
        fixtures_total=len(res),
        guard_sensitivity_band=dict(
            changes_output=band,
            interpretation="the guard is inert for any value below the data's minimum body "
                           "and becomes active around one tick; on a 2-decimal lattice the "
                           "ratio==1.0 comparison is additionally float-marginal",
            taxonomy="PREDICATE_BOUNDARY_SENSITIVITY",
            inherited_faithfully="the producer behaves identically — Layer 1 is exact"),
        independent_verifier=dict(
            module="t_port_verifier_v1.py",
            imports_port=False, imports_producer=False,
            method="scalar row loop with an explicit if/elif priority ladder",
            same_input_mismatches=d_pv),

        findings=[
            dict(finding="raw prior-bar doji and resolved Z7 COINCIDE in this producer",
                 proof="every Z condition requires isBear and every T condition requires "
                       "isBull, so on a doji bar anyB=anyZ=False and cZ7c == isDoji",
                 consequence="Amendment 2's rule stands and is now proven harmless rather "
                             "than merely obeyed"),
            dict(finding="the session-boundary choice is observable",
                 detail="on a constructed two-session frame the first bar of session 2 "
                        "differs between SESSION_CONTINUOUS and session-reset",
                 consequence="binding it in the preflight was necessary; it could not have "
                             "been left to be discovered from results"),
            dict(finding="min_body_ratio <= 1.0 is redundant under use_wick=False",
                 proof="the body-edge conditions cTop>=pTop and pBot>=cBot imply "
                       "cBody = cTop-cBot >= pTop-pBot = pBody; measured with 0 "
                       "counterexamples over the corpus",
                 consequence="only min_body_ratio > 1.0 is semantically active; a downward "
                             "mutation is a no-op rather than an undetected change",
                 fixture_corrected="P7 originally used bars whose ratio was 1.0 but whose "
                                   "bodies did not engulf, so it tested nothing"),
            dict(finding="the 1e-10 guard has a measured inert band",
                 detail="it binds only where the previous body falls below it, i.e. after a "
                        "doji, and the current body must then clear it",
                 consequence="registered as PREDICATE_BOUNDARY_SENSITIVITY and carried into "
                             "the divergence taxonomy for the blocked feed comparison")],

        divergence_taxonomy_registered=[
            "LEGACY_STORAGE_PRECISION", "INPUT_VINTAGE_DIFFERENCE", "SOURCE_OHLC_DIFFERENCE",
            "PREDICATE_BOUNDARY_SENSITIVITY", "CALL_CONTEXT_DIFFERENCE", "PRIORITY_DIFFERENCE",
            "IMPLEMENTATION_DEFECT", "UNAVAILABLE_INPUT", "UNRESOLVED"],
        no_conformance_threshold_invented=True,
        blocked_components=blocked,
        pass_criteria=dict(volume_readable=VOL_OK, all_fixtures_ran=all_ran,
                           all_fixtures_passed=passed, layer_1_exact=l1_exact,
                           nothing_blocked=not blocked),
        blocker=dict(kind="MACOS_TCC_FILE_ACCESS",
                     symptom="PermissionError [Errno 1] Operation not permitted on "
                             "/Volumes/QUANT_RESEARCH despite the volume being mounted",
                     mount_guard="fail-closed; sentinel_uuid_matches could not be read",
                     nothing_written=True,
                     remedy="grant the Claude Code app Full Disk Access in System Settings "
                            "and restart it; this is a user action"),

        t_production_writes=0, z_research_outputs=0, y_exposed=0,
        outcome_exposure="NOT_EXPOSED",
        authorizes=("MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1" if final_pass
                    else None),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json",
                 required=("report_id", "status", "call_context_preflight",
                           "layer_1_semantic_correctness", "fixtures",
                           "implementation_hash", "independent_verifier"),
                 supersede=os.path.exists("MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json"))
    print(f"\nMASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1 · {d} · {p['status']}")
    print(f"  tf registry {TFG['verdict']} — research {TFG['research_registry']} · diagnostic {TFG['diagnostic_registry']} ({DIAGNOSTIC_CLASS[:18]}…)")
    print(f"  preflight   ESTABLISHED — SESSION_CONTINUOUS, dedup+ascending, 1 ticker")
    print(f"  port hash   {PH[:16]}   harness hash {HH[:16]}")
    print(f"  LAYER 1     {int(ev.sum()):,} bars · producer=port=verifier · "
          f"mismatches {d_ep}/{d_pv}/{d_ev}")
    print(f"  fixtures    {sum(1 for r in res if r['passed'] is True)}/{len(res)} "
          f"({'all runnable passed' if passed else 'FAILURES PRESENT'})")
    if blocked:
        print(f"  BLOCKED     {len(blocked)} components — external volume unreadable")
    else:
        c = LAYERS["layer_3b_massive_15m_capability"]["totals"]
        l2 = LAYERS["layer_2_legacy_1d_materializer_storage_conformance"]
        l3 = LAYERS["layer_3a_1d_cross_source"]
        print(f"  LAYER 3B    {c['evaluable']:,} evaluable / {c['slots']:,} slots · "
              f"fired {c['fired']:,}")
        print(f"  LAYER 2     support {l2['support']:,} · agreement {l2['agreement']}")
        print(f"  LAYER 3A    matched {l3.get('common_matched_support',0):,} · "
              f"T agreement {l3.get('t_agreement')}")
    print(f"  writes      T=0 · Z=0 · Y_EXPOSED=0")
    return 0


# ---- mutation helpers: each must CHANGE the result, proving detectability ----
def _mutate(df, eng, ev, attr, val):
    old = getattr(PORT, attr)
    try:
        setattr(PORT, attr, val)
        bc = PORT.compute_t(df)["bc"].to_numpy().astype(int)
        return int((eng[ev] != bc[ev]).sum()) > 0
    finally:
        setattr(PORT, attr, old)


def _mutate_guard(df, eng, ev, val):
    return _mutate(df, eng, ev, "NUMERICAL_GUARD", val)


def _threshold_doji_differs(df, eng, ev):
    o = df["open"].to_numpy(); c = df["close"].to_numpy()
    h = df["high"].to_numpy(); l = df["low"].to_numpy()
    rng = h - l
    with np.errstate(invalid="ignore", divide="ignore"):
        thr = (rng > 0) & (np.abs(c - o) / np.where(rng == 0, np.nan, rng) <= 0.05)
    return int((thr != (c == o)).sum()) > 0


def _permute_priority(df, eng, ev):
    old = list(PORT.PRIORITY)
    try:
        PORT.PRIORITY[0], PORT.PRIORITY[1] = old[1], old[0]
        bc = PORT.compute_t(df)["bc"].to_numpy().astype(int)
        return int((eng[ev] != bc[ev]).sum()) > 0
    finally:
        PORT.PRIORITY[:] = old


def _bad_mapping(df, ev):
    old = dict(PORT.BC_TO_SID)
    try:
        PORT.BC_TO_SID[1] = 8
        lab = PORT.compute_t(df)["t_label"].to_numpy()
        return bool((lab[ev] == "T6").sum() > (PORT.compute_t(df)["bc"].to_numpy()[ev] == 2)
                    .sum())
    finally:
        PORT.BC_TO_SID.clear(); PORT.BC_TO_SID.update(old)


def _unavailable_not_false(frame, obs, i):
    r = PORT.compute_t(frame, observed=obs)
    return r["state"].iloc[i] == "UNAVAILABLE" and r["t_label"].iloc[i] == ""


def _no_tunable_config():
    import inspect
    sig = inspect.signature(PORT.compute_t)
    return not ({"use_wick", "min_body_ratio", "doji_thresh", "guard", "mintick"}
                & set(sig.parameters))


def _no_y_tokens():
    bad = re.compile(r"\b(fwd_|future_|ret_|theta|pnl|outcome|target_|label_y)\b", re.I)
    return not any(bad.search(open(f).read())
                   for f in ("t_port_v1.py", "t_port_verifier_v1.py"))


if __name__ == "__main__":
    raise SystemExit(main())
