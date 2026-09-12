"""UDN_CONFLICT_V1 — when the two honest readings of the same session CONTRADICT each other,
is that a state worth anything? User-approved 2026-09-10 ("ki"). k = 5.

  THE QUESTION, and why it is new. L-BAL decomposes a 1D session into its 15m bars and computes the
  effort line UDN two ways: `V` counts only 15m bars whose volume > SMA20 (the TradingView setting),
  `all` counts every labelled bar. Both are honest; neither is the "right" one. Measured over
  3,652,833 sessions while splitting them onto separate chart rows, they disagree on 55.6 % of
  sessions and INVERT OUTRIGHT — one says U, the other says D — on 13.0 %.
  Nothing in the book has ever looked at the DISAGREEMENT itself. It was hidden behind a UI toggle,
  so it could not be seen, let alone tested.

  MECHANISM (declared, and it is why this got the k). `V` restricts to the bars where volume showed
  up. `V = U while all = D` therefore means: the EFFORT is where the VOLUME is, even though most of
  the labelled bars point down — buying is happening on the bars that matter and selling on the
  quiet ones. That is the fingerprint of absorption, and absorption is the one confluence the book
  has repeatedly validated ([[project_what_actually_works]]). The mirror case, `V = D while all = U`,
  is distribution dressed as strength.

  Cells    ALL_BARS (the reference) · AGREE (udn == udn_all) · INVERT_V_up (V=U, all=D) ·
           INVERT_V_dn (V=D, all=U) · PARTIAL (one line neutral, the other not).  k = 5.
           The two inversions are kept SEPARATE, never pooled: if the effect is directional,
           pooling cancels it — the exact failure the user caught on the swallow shapes, where
           LST|UP was veto-grade and LST|DN was flat, and the pooled cell said nothing.
  Sampling This is a STATE, not a rare event: every liquid session has one. Path-sim over 2.8 M
           bars per cell is not runnable, so EVERY cell is thinned identically by a deterministic
           1-in-8 per-ticker grid at a sha256 offset (control_keys.phase, never python hash()).
           Stride 8 > the engine's 5-bar cooldown, so no selection is suppressed
           ([[feedback-pathsim-cooldown-estimand]]). Thinning costs precision, not bias, and each
           cell is judged against the same-day CONTROL rather than against another cell.
  Prices   L-BAL states joined from data/lbal_signals.parquet at (ticker, session); trades run on
           the canonical 1D authority.
  Universe close >= 21, avg_vol_20d > 0, close*volume >= 3M, universe <> index.
  Control  CONTROL_KEYS v2 (de-phased); per entry date the control-day median; day-clustered.
  Estimand direct sacred `edge_replay._pathsim` over the registered mask, engine cooldown included.
  OOS      MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  Stop     A directional claim only if the sign holds in BOTH windows. Otherwise NULL. One outcome
           run, no cell re-cut, no stride search.

  DECLARED PRIOR: the strongest of the three questions on the table. The mechanism is independent of
  anything already in the book, the base is enormous (13 % of 3.65 M sessions invert), and the axis
  has never been looked at. Against it: UDN's own research family INTRADAY_EFFORT_BALANCE_V1 closed
  16/16 NULL — the effort DIRECTION added nothing. This asks about the CONTRADICTION between two
  measurements of it, which is a different object, but the parent family's result is a real warning.
"""
from __future__ import annotations
import os, sys, json, time, hashlib, glob, gc
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import HardStop, RunSpace                                 # noqa: E402
import ovd_build as B                                                    # noqa: E402
import ovd_outcome_v1 as O                                               # noqa: E402
import ovd_outcome_direct_v1 as OD                                       # noqa: E402
import control_keys as CK                                                # noqa: E402
from breadth_family import GATES, classify, _stats_window                # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/UDN_CONFLICT_V1"
FAMILY = "UDN_CONFLICT_V1"
DB = os.path.join(os.path.dirname(HERE), "data", "studio_analytics.duckdb")
LBAL = os.path.join(os.path.dirname(HERE), "data", "lbal_signals.parquet")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED = 5
STRIDE = 8                # > the engine's 5-bar cooldown, so thinning suppresses nothing
RSI_BANDS = ((-1e9, 35.0, "<35"), (35.0, 50.0, "35-50"), (50.0, 60.0, "50-60"), (60.0, 1e9, ">=60"))


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def cells():
    return [
        dict(claim_id="UDN|ALL_BARS", kind="all", rule="every sampled liquid session with both readings"),
        dict(claim_id="UDN|AGREE", kind="agree", rule="udn == udn_all (the two counts say the same thing)"),
        dict(claim_id="UDN|INVERT_V_up", kind="inv_up", rule="V says U and all says D — effort where the volume is"),
        dict(claim_id="UDN|INVERT_V_dn", kind="inv_dn", rule="V says D and all says U — distribution dressed as strength"),
        dict(claim_id="UDN|PARTIAL", kind="partial", rule="exactly one of the two readings is N"),
    ]


def cell_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    v, a = X["udn"].to_numpy(), X["udn_all"].to_numpy()
    k = c["kind"]
    if k == "all":
        return np.ones(len(X), bool)
    if k == "agree":
        return v == a
    if k == "inv_up":
        return (v == "U") & (a == "D")
    if k == "inv_dn":
        return (v == "D") & (a == "U")
    return ((v == "N") ^ (a == "N")) & (v != a)


def _band(v: np.ndarray) -> np.ndarray:
    out = np.full(len(v), "", dtype=object)
    for lo, hi, lab in RSI_BANDS:
        out[(v >= lo) & (v < hi)] = lab
    return out


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    if not os.path.exists(LBAL):
        raise HardStop("lbal_signals.parquet missing — run backend/lbal_build.py")
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("UC_%Y%m%dT%H%M%SZ", time.gmtime()))

    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "close", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    px = px[(px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
            & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1])]
    px = px.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    log(f"  liquid sessions {len(px):,} · {px['ticker'].nunique():,} tickers")

    import duckdb
    con = duckdb.connect()
    try:
        con.register("keys", px[["ticker", "session"]])
        lb = con.execute(f"""
            SELECT k.ticker, k.session, p.udn, p.udn_all
            FROM keys k JOIN read_parquet('{LBAL}') p
              ON p.ticker = k.ticker AND CAST(p.date AS VARCHAR) = k.session
            WHERE p.udn IS NOT NULL AND p.udn_all IS NOT NULL""").fetchdf()
    finally:
        con.close()
    log(f"  with BOTH readings present: {len(lb):,}")
    del px; gc.collect()

    # deterministic 1-in-STRIDE per ticker, sha256 offset — identical treatment for every cell
    lb = lb.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    pos = lb.groupby("ticker", sort=False).cumcount().to_numpy()
    off = lb["ticker"].map(lambda t: CK.phase(t, STRIDE)).to_numpy()
    X = lb[(pos % STRIDE) == off].reset_index(drop=True)
    log(f"  after the 1-in-{STRIDE} de-phased thinning: {len(X):,}")
    del lb; gc.collect()

    con = duckdb.connect(DB, read_only=True)
    try:
        con.register("k2", X[["ticker", "session"]])
        st = con.execute("""
            WITH r AS (SELECT b.ticker, CAST(b.date AS VARCHAR) AS session, b.rsi_14,
                              row_number() OVER (PARTITION BY b.ticker, b.date ORDER BY b.universe) rn
                       FROM bars b JOIN k2 k ON k.ticker = b.ticker AND CAST(b.date AS VARCHAR) = k.session
                       WHERE b.universe <> 'index')
            SELECT * EXCLUDE (rn) FROM r WHERE rn = 1""").fetchdf()
    finally:
        con.close()
    st["session"] = st["session"].str[:10]
    X = X.merge(st, on=["ticker", "session"], how="left")
    X["rsi_band"] = _band(pd.to_numeric(X["rsi_14"], errors="coerce").to_numpy(float))
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)

    cs = {}
    for c in cells():
        m = cell_mask(X, c)
        cs[c["claim_id"]] = dict(TRUE=int(m.sum()), TRUE_mine=int((m & X["in_mine"]).sum()),
                                 TRUE_verify=int((m & X["in_verify"]).sum()))
    pair = X.groupby(["udn", "udn_all"]).size().sort_values(ascending=False)
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, px_min=PX_MIN, dv_floor=DV_FLOOR,
               signal=dict(name="UDN_CONFLICT",
                           question="does the DISAGREEMENT between the V and all readings of UDN pay?",
                           definition="V counts only 15m bars with volume > SMA20; all counts every "
                                      "labelled bar. Inversion = one says U and the other says D.",
                           mechanism="V=U with all=D means the effort sits on the bars that carried "
                                     "volume while the majority pointed down — the absorption "
                                     "fingerprint. The mirror is distribution dressed as strength.",
                           sampling=f"deterministic 1-in-{STRIDE} per ticker at a sha256 offset "
                                    f"(control_keys.phase); stride > the 5-bar cooldown so nothing is "
                                    f"suppressed; identical for every cell",
                           source="data/lbal_signals.parquet (lbal_build.py)"),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               sources=dict(studio_db=DB, lbal_parquet=LBAL, lbal_sha256_16=_dig(LBAL)),
               census=dict(rows=int(len(X)), tickers=int(X["ticker"].nunique()),
                           days=int(X["session"].nunique()),
                           rows_per_year={int(y): int(n) for y, n in X.groupby(X["session"].str[:4]).size().items()},
                           rsi_band_shares=X["rsi_band"].value_counts(normalize=True).round(4).to_dict(),
                           udn_pair_census={f"{a}|{b}": int(n) for (a, b), n in pair.items()},
                           cells=cs),
               x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    log(f"X {len(X):,} sampled sessions · " + " · ".join(
        f"{k.split('|')[1]} {v['TRUE_mine']}/{v['TRUE_verify']}" for k, v in cs.items()))
    log(f"  udn pairs (V|all): {dict(list(rep['census']['udn_pair_census'].items())[:9])}")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "UC_*")), reverse=True):
        if RunSpace.is_current(r, "X.parquet"):
            return r
    return None


def seal() -> dict:
    if os.path.exists(os.path.join(FAMILY_DIR, "SEAL.json")):
        raise HardStop("SEAL.json exists")
    if any(k in f.lower() for _, _, fs in os.walk(FAMILY_DIR) for f in fs for k in ("outcome", "result", "pathsim")):
        raise HardStop("outcome-bearing file present")
    run = latest_run()
    if not run:
        raise HardStop("no COMPLETED X build")
    rep = json.load(open(os.path.join(run, "build_report.json")))
    if _dig(os.path.join(run, "X.parquet")) != rep["x_sha256_16"]:
        raise HardStop("X digest mismatch")
    ps, slip = O.sacred_pathsim()
    cen = rep["census"]
    lim = dict(cell_counts={k: v["TRUE_mine"] for k, v in cen["cells"].items()},
               udn_pair_census=cen["udn_pair_census"], rsi_band_shares=cen["rsi_band_shares"],
               sampling=f"1-in-{STRIDE} de-phased thinning applied identically to every cell; it costs "
                        f"precision, not bias, and each cell is judged against the same-day control "
                        f"rather than against another cell",
               declared_prior="The strongest of the three questions on the table: an independent "
                              "mechanism, an enormous base (13 % of 3.65 M sessions invert) and an axis "
                              "never looked at, because a UI toggle hid it. AGAINST it: UDN's own "
                              "family INTRADAY_EFFORT_BALANCE_V1 closed 16/16 NULL on the effort "
                              "DIRECTION. This tests the CONTRADICTION, a different object, but that "
                              "result is a real warning.",
               note="The two inversions are separate cells and must NOT be pooled in reading the "
                    "result: a directional effect would cancel, which is exactly how the swallow "
                    "shapes hid a veto-grade cell inside a flat pooled one.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", known_limitations=lim,
               sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-10 (\"ki\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "A_PRIORI — the axis was surfaced by splitting the chart rows, not by any outcome"],
               question=rep["signal"]["question"], signal=rep["signal"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}, universe <> index",
               control=f"CONTROL_KEYS v2 de-phased ({rep['control']['run_id']}); per entry date the "
                       f"control-day median (>= {O.MIN_CONTROL}); edge = trade - control median; day-clustered",
               estimand="direct sacred edge_replay._pathsim over the registered mask, engine cooldown INCLUDED",
               oos=OOS, gates=GATES,
               multiplicity=dict(k=K_EXPECTED, rule="DSR with the 5 cells' MINE day-edge Sharpes; n_trials = 5"),
               cells=cells(), descriptives="RSI composition and the UDN pair census — never cells",
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], sources=rep["sources"], census=cen,
               stop_rule="a directional claim only if the sign holds in BOTH windows; otherwise NULL. "
                         "One outcome run; no cell re-cut; no stride search; BUILD only on a second user OK")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(udn_conflict_family=_dig(__file__), edge_replay=_dig(os.path.join(HERE, "edge_replay.py")),
                       control_keys=_dig(os.path.join(HERE, "control_keys.py")), ovd_outcome_v1=_dig(O.__file__),
                       ovd_outcome_direct_v1=_dig(OD.__file__)),
             outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


def _day_control(ctrl: pd.DataFrame, obs: pd.DataFrame):
    med = ctrl.groupby("date_in")["ret"].median(); cnt = ctrl.groupby("date_in")["ret"].size()
    return obs["date_in"].map(med).to_numpy(float), obs["date_in"].map(cnt).fillna(0).to_numpy(int)


def outcome(log=print) -> dict:
    sp = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(sp):
        raise HardStop("not sealed")
    s = json.load(open(sp)); reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")))
    if _dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")) != s["registry_sha256_16"] or len(reg["cells"]) != s["k"]:
        raise HardStop("registry digest / k mismatch")
    run_x = os.path.join(FAMILY_DIR, "runs", s["x_run"])
    if _dig(os.path.join(run_x, "X.parquet")) != s["x_sha256_16"]:
        raise HardStop("X mismatch")
    cur = B.canonical_current()
    if cur["derived_sha256_16"] != s["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical changed")
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    if man["sha256_16"] != s["control"]["sha256_16"]:
        raise HardStop("control artifact changed since sealing")
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "udn")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                              seal_registry=s["registry_sha256_16"]))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")

    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    ctl = CK.load(man); ctl["session"] = ctl["session"].astype(str).str[:10]
    ctl = ctl[(ctl["session"] >= OOS["MINE"][0]) & (ctl["session"] <= OOS["VERIFY"][1])].reset_index(drop=True)
    frames = OD.load_frames(cur["derived_parquet"], pd.concat([X["ticker"], ctl["ticker"]]).unique())
    trades, cons = {}, {}
    for c in reg["cells"]:
        m = cell_mask(X, c)
        tr, cn = OD.direct_trades(ps, frames, X.loc[m, ["ticker", "session"]])
        tr["cell_id"] = c["claim_id"]; trades[c["claim_id"]] = tr; cons[c["claim_id"]] = cn
        log(f"  {c['claim_id']}: signal {cn.get('signal_true_n', '?')} -> trades {len(tr)}", flush=True)
    ctr, ccn = OD.direct_trades(ps, frames, ctl[["ticker", "session"]])
    cons["CONTROL"] = ccn; log(f"  control: {ccn.get('signal_true_n', '?')} -> trades {len(ctr)}")
    run.write_atomic("selection_conservation.json", cons)
    ctr = ctr[ctr["ret"].notna()]
    cn = ctr.groupby("date_in")["ret"].size()
    cov = {}
    for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
        sl = cn[(cn.index >= lo) & (cn.index <= hi)]
        cov[w] = dict(sessions=int(len(sl)), median_per_day=float(sl.median()) if len(sl) else None,
                      share_ge_min=round(float((sl >= O.MIN_CONTROL).mean() * 100), 1) if len(sl) else None)
    log(f"  control coverage: {cov}")

    results, mine_series = [], {}
    for cid, tr in trades.items():
        obs = tr[tr["ret"].notna()].copy()
        if len(obs):
            cv, n = _day_control(ctr, obs)
            obs["control"] = cv; obs["n_others"] = n
            obs["edge"] = np.where(obs["n_others"] >= O.MIN_CONTROL, obs["ret"] - obs["control"], np.nan)
        mine = _stats_window(obs, *OOS["MINE"]); ver = _stats_window(obs, *OOS["VERIFY"])
        results.append(dict(claim_id=cid, **{k: v for k, v in cons[cid].items()},
                            mine={k: v for k, v in mine.items() if k != "day_series"},
                            verify={k: v for k, v in ver.items() if k != "day_series"}))
        mine_series[cid] = mine["day_series"]
    from overfit_stats import dsr, sharpe, psr
    kids = [r["claim_id"] for r in results]
    if len(kids) != K_EXPECTED:
        raise HardStop(f"k mismatch: {len(kids)} != {K_EXPECTED}")
    trial = [sharpe(mine_series[k].to_numpy()) if len(mine_series[k]) >= 2 else 0.0 for k in kids]
    for r in results:
        srs = mine_series[r["claim_id"]].to_numpy(float)
        if len(srs) >= 3:
            dp = dsr(srs, trial, n_trials=K_EXPECTED); dn = dsr(-srs, [-x for x in trial], n_trials=K_EXPECTED)
            r.update(dsr=dp["dsr"], sr=dp["sr"], sr_star=dp["sr_star"], dsr_neg=dn["dsr"], psr0=round(float(psr(srs, 0.0)), 4))
        else:
            r.update(dsr=None, sr=None, sr_star=None, dsr_neg=None, psr0=None)
        r["classification"] = classify(r["mine"], r["verify"], r["dsr"], r["dsr_neg"])
        m, v = r["mine"], r["verify"]
        log(f"  {r['claim_id']:20s} MINE med {m['median_edge']} win {m['day_win']} yrs {m['positive_years']}/{m['years_counted']} "
            f"worst {m['worst_year']} raw {m.get('raw_median')} | VERIFY med {v['median_edge']} win {v['day_win']} "
            f"worst {v['worst_year']} raw {v.get('raw_median')} | DSR {r['dsr']} | {r['classification']}")
    # the registered contrast: each inversion against AGREE, both windows
    def _m(cid, w):
        return next((r[w]["median_edge"] for r in results if r["claim_id"] == cid), None)
    contrast = {}
    for cid in ("UDN|INVERT_V_up", "UDN|INVERT_V_dn"):
        g = {}
        for w in ("mine", "verify"):
            x, a = _m(cid, w), _m("UDN|AGREE", w)
            g[w] = None if (x is None or a is None) else round(x - a, 3)
        g["same_sign"] = (g["mine"] is not None and g["verify"] is not None and g["mine"] * g["verify"] > 0)
        contrast[cid] = g
        log(f"  CONTRAST {cid} minus AGREE: MINE {g['mine']} · VERIFY {g['verify']} · same_sign={g['same_sign']}")
    verdict = ("DIRECTIONAL:" + ",".join(k for k, v in contrast.items() if v["same_sign"])) \
        if any(v["same_sign"] for v in contrast.values()) else "NO_STABLE_CONTRAST"
    log(f"  VERDICT: {verdict}")

    run.write_atomic("results.json", results)
    cls = {}
    for r in results:
        cls[r["classification"]] = cls.get(r["classification"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED,
                oos=OOS, gates=GATES, control=s["control"], control_coverage=cov,
                pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, outcome_access_count=led["outcome_access_count"],
                classification_census=cls, contrast_vs_agree=contrast, verdict=verdict,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id,
                   seal_registry_sha256_16=s["registry_sha256_16"],
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")),
                   outcome_access_count=led["outcome_access_count"], pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                   classification_census=cls, verdict=verdict, status="COMPLETE — family SEARCH-EXPOSED"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"classification: {cls} · {verdict}")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items()
                                       if k in ("sealed_at", "registry_sha256_16", "k", "x_run", "x_sha256_16")}, indent=1)),
     "outcome": outcome}[cmd]()
