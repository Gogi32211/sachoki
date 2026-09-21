"""PV_MULTI_V1 — nine PRICE x VOLUME ordinal shapes from the user's 260921 Pine script. k = 9.
User-approved 2026-09-21 ("gaushvi" to the 5-line plan).

  THE QUESTION. Every volume feature the app stores is a LEVEL: vol_bucket W/L/N/B/VB against a
  20-bar band, VOL7 M0..M6 against the 20-day median, sig_vol_5x/10x/20x against the average. The
  439-column 1D store holds NOT ONE ordinal volume-DIRECTION feature, and the single `rising3` in
  the whole codebase (ovd_map_build.py:120) runs on same-slot opening RVOL, not on raw daily volume.
  These nine shapes are therefore a genuinely untested object — which is the only reason to spend a
  k on them, because their NEIGHBOURHOOD is uniformly NULL:
      UPP/UPR ~ OB.30/OB.60    OPENING_VOLUME_DYNAMICS V1/V2.1, k=300  -> 283 NULL, 0 BUILD
      UP4/RE2 ~ burst-quiet-burst  BQB_V1, k=6                         -> NULL
      TURN    ~ TURN_V1 k=3 -> 3/3 fail; ADJACENT_TURN_V1 k=1          -> VERIFY -0.08 (p 0.82)
      DIV     ~ effort/result   INTRADAY_EFFORT_BALANCE_V1, k=16       -> 16/16 NULL

  THE CENSUS, read BEFORE sealing (2.95M liquid ticker-sessions, contract clean, adjacency 99.69%):
      DIV 5.95% · UPP 5.97% · UPR 3.58% · REV 1.64% · RUP 3.83% · VUP 10.55% · TURN 0.74% ·
      UP4 1.42% · RE2 2.19%   — 35.9% of bars carry exactly one of the nine.
    The nine are MUTUALLY EXCLUSIVE by construction (every pair contradicts on the price or the
    volume leg), so same-bar confluence does not exist and only succession was ever askable. That
    was measured too, descriptively: every A->B lift sits at lag 1-2 and collapses to 1.00 from lag 3
    on, with DIV->RE2 at 0.00 and TURN->TURN at 0.00 — window overlap, i.e. arithmetic. No sequence
    cell is registered here, because there is nothing to register.
    ALSO from the census: "volume rising three days" is NOT "high volume". Median RVOL on a firing
    bar is 1.33-1.43 and only 12-15% land in the app's B/VB buckets; VUP's median RVOL is 0.81 with
    76% BELOW the 20-day median. These shapes select the MIDDLE of the volume distribution, which is
    precisely the region the level vocabulary says nothing about.

  DEVIATIONS FROM THE APPROVED 5 LINES — declared, not silent:
    1. Price floor is close >= 21, not >= 5. CONTROL_KEYS v2 is built at close >= 21, and a cell
       measured on a universe its control does not cover is the configuration that made "buy every
       bar" a BUILD_CANDIDATE in UDN_CONFLICT_V1. The family follows the control, not the reverse.
    2. Window is 2021-09-07..2026-09-03, the control artifact's own window, not 2021-05.
    3. No 1-in-N sampling: the registered masks run DIRECTLY. ADJACENT_TURN_V1 showed a 1-in-12
       thin sample manufacturing an apparent replication that 12x more data erased, and the
       cooldown estimand forbids mod-N partitions around the engine's 5-bar rule.

  Cells (k = 9): one per shape, as written in the script, on OHLC4 ("3 observations" = 3 bars = 2
  steps, the script's own convention). No variant, no threshold, no ladder, no re-cut.
  Control  CONTROL_KEYS v2 (de-phased), RESTRICTED to the tickers the family covers; per entry date
           the control-day median, needing >= MIN_CONTROL other trades.
  Placebo  size-matched draws from the control trades themselves (signal-free by construction),
           day-clustered exactly like the cell. A cell must clear its own noise band, not just zero.
  Estimand direct sacred edge_replay._pathsim, engine cooldown INCLUDED, entry D+1 open.
  OOS      MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  Gates    the book standard (median>0 · day-win>50 · >=3 positive years · worst >= -2 · DSR >= 0.95
           at k=9 · not THIN) AND placebo p < 0.05 in BOTH windows.
  Stop     one outcome run; nothing passes and replicates -> NULL, the family closes and every later
           idea born from these outcomes is a POST_EXPOSURE_HYPOTHESIS.
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
from breadth_family import GATES as _BOOK_GATES, _stats_window           # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/PV_MULTI_V1"
FAMILY = "PV_MULTI_V1"
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED = 9
NPLACEBO, RNG_SEED = 200, 20260921
GATES = dict(_BOOK_GATES, k=K_EXPECTED,
             placebo=dict(p_lt=0.05, n=NPLACEBO, both_windows=True,
                          source="size-matched draws from the CONTROL trades (signal-free)"))

SIGNALS = [
    ("DIV",  "price fell 3 bars while volume rose 3 bars"),
    ("UPP",  "price rose 3 bars while volume rose 3 bars"),
    ("UPR",  "volume rose 3 bars and price is above its level 2 bars ago, excluding clean UPP"),
    ("REV",  "price and volume both declined 3 bars, then both turned up"),
    ("RUP",  "volume contracted 3 bars with day-3 price above day-1, then price and volume turned up"),
    ("VUP",  "volume fell 3 bars while price stayed above its level 2 bars ago"),
    ("TURN", "price declined then recovered 2 bars, with the script's volume turn structure"),
    ("UP4",  "price rose 4 bars while volume expanded, rested, then re-expanded above the prior peak"),
    ("RE2",  "price down-down-up while volume went up-down-up"),
]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def signal_masks(d: pd.DataFrame) -> dict:
    """The nine, ported VERBATIM from the Pine on OHLC4. d: one frame sorted by (ticker, session)."""
    price = (d["open"] + d["high"] + d["low"] + d["close"]) / 4.0
    vol = d["volume"].astype(float)
    sh = lambda s, k: s.groupby(d["ticker"], sort=False).shift(k)
    p, p1, p2, p3, p4 = price, sh(price, 1), sh(price, 2), sh(price, 3), sh(price, 4)
    v, v1, v2, v3, v4 = vol, sh(vol, 1), sh(vol, 2), sh(vol, 3), sh(vol, 4)
    priceDown3 = (p2 > p1) & (p1 > p)
    priceUp3 = (p2 < p1) & (p1 < p)
    volUp3 = (v2 < v1) & (v1 < v)
    S = {}
    S["DIV"] = priceDown3 & volUp3
    S["UPP"] = priceUp3 & volUp3
    S["UPR"] = (p > p2) & volUp3 & ~S["UPP"]
    S["REV"] = (p3 > p2) & (p2 > p1) & (v3 > v2) & (v2 > v1) & (p > p1) & (v > v1)
    S["RUP"] = (v3 > v2) & (v2 > v1) & (p1 > p3) & (p > p1) & (v > v1)
    S["VUP"] = (v2 > v1) & (v1 > v) & (p > p2)
    S["TURN"] = ((p4 > p3) & (p3 > p2) & (p1 > p2) & (p > p1)
                 & (v4 > v3) & (v2 > v3) & (v1 < v2) & (v > v1))
    S["UP4"] = ((p3 < p2) & (p2 < p1) & (p1 < p)
                & (v3 < v2) & (v1 < v2) & (v > v1) & (v > v2))
    S["RE2"] = (p3 > p2) & (p2 > p1) & (p > p1) & (v2 > v3) & (v1 < v2) & (v > v1)
    # NO global warm-up mask. Each shape carries its OWN depth and every comparison is strict, so a
    # NaN from the groupby-shift is already False: DIV/UPP/UPR/VUP become legal on a ticker's 3rd
    # bar and TURN on its 5th, exactly as the Pine does. An earlier draft here required four prior
    # bars for all nine, which silently suppressed the shallow shapes at each series start — a
    # deviation from the script that no downstream statistic could have revealed.
    return {k: m.fillna(False).to_numpy() for k, m in S.items()}


def frame(cur, log=print) -> pd.DataFrame:
    """The whole liquid 1D frame. Shapes are computed on the ticker's FULL bar sequence and the
    liquidity floor is applied afterwards — a floor that deletes bars from the middle of a history
    would otherwise make a '3-bar sequence' three quarters wide (data_contract, 2026-08-09)."""
    d = pd.read_parquet(cur["derived_parquet"],
                        columns=["ticker", "session_date", "open", "high", "low", "close",
                                 "volume", "avg_vol_20d", "dollar_vol"])
    d["session"] = d["session_date"].astype(str).str[:10]
    d = d.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    dup = len(d) - len(d[["ticker", "session"]].drop_duplicates())
    if dup:
        raise HardStop(f"{dup:,} duplicate (ticker,session) rows in the canonical frame")
    M = signal_masks(d)
    for k, m in M.items():
        d[k] = m
    d["eligible"] = ((d["close"] >= PX_MIN) & (d["dollar_vol"] >= DV_FLOOR) & (d["avg_vol_20d"] > 0)
                     & (d["session"] >= OOS["MINE"][0]) & (d["session"] <= OOS["VERIFY"][1]))
    log(f"  canonical rows {len(d):,} · eligible {int(d['eligible'].sum()):,} · "
        f"tickers {d.loc[d['eligible'], 'ticker'].nunique():,}")
    return d


def cell_mask(X: pd.DataFrame, claim_id: str) -> np.ndarray:
    return X[claim_id].to_numpy(bool) & X["eligible"].to_numpy(bool)


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("PV_%Y%m%dT%H%M%SZ", time.gmtime()))
    d = frame(cur, log)
    keep = d["eligible"].to_numpy() & np.any([d[k].to_numpy(bool) for k, _ in SIGNALS], axis=0)
    X = d.loc[keep, ["ticker", "session"] + [k for k, _ in SIGNALS] + ["eligible"]].reset_index(drop=True)
    exclusive = int((X[[k for k, _ in SIGNALS]].sum(axis=1) > 1).sum())
    if exclusive:
        raise HardStop(f"{exclusive:,} bars carry more than one of the nine — they are not exclusive")
    census = {}
    for k, _ in SIGNALS:
        m = cell_mask(X, k)
        census[k] = dict(fires=int(m.sum()), tickers=int(X.loc[m, "ticker"].nunique()),
                         days=int(X.loc[m, "session"].nunique()),
                         mine=int((m & (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])).sum()),
                         verify=int((m & (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])).sum()))
        log(f"  {k:5s} {census[k]['fires']:>8,} fires · {census[k]['tickers']:>5,} tickers · "
            f"MINE {census[k]['mine']:>7,} · VERIFY {census[k]['verify']:>7,}")
    del d; gc.collect()
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, px_min=PX_MIN, dv_floor=DV_FLOOR,
               n_placebo=NPLACEBO, rng_seed=RNG_SEED,
               signal=dict(name="PV_MULTI", source="user Pine 260921 PRICE x VOLUME — Multi Signal",
                           price="OHLC4", question="do the nine ordinal price x volume shapes beat an "
                                                   "ordinary bar of the same day?",
                           cells=[dict(claim_id=k, rule=r) for k, r in SIGNALS],
                           mutually_exclusive=True, sampling="NONE — registered masks run directly"),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               census=census, x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "PV_*")), reverse=True):
        if RunSpace.is_current(r, "X.parquet"):
            return r
    return None


def seal() -> dict:
    if os.path.exists(os.path.join(FAMILY_DIR, "SEAL.json")):
        raise HardStop("SEAL.json exists")
    if any(k in f.lower() for _, _, fs in os.walk(FAMILY_DIR) for f in fs
           for k in ("outcome", "result", "pathsim")):
        raise HardStop("outcome-bearing file present")
    run = latest_run()
    if not run:
        raise HardStop("no COMPLETED X build")
    rep = json.load(open(os.path.join(run, "build_report.json")))
    if _dig(os.path.join(run, "X.parquet")) != rep["x_sha256_16"]:
        raise HardStop("X digest mismatch")
    ps, slip = O.sacred_pathsim()
    lim = dict(
        prior="the neighbourhood is uniformly NULL: OPENING_VOLUME_DYNAMICS k=300 (283 NULL), "
              "BQB_V1 k=6 NULL, TURN_V1 k=3 fail, ADJACENT_TURN_V1 VERIFY -0.08, "
              "INTRADAY_EFFORT_BALANCE_V1 16/16 NULL. What is new is the ORDINAL volume direction; "
              "the app stores only volume LEVELS.",
        base_rates="census read before sealing: VUP fires on 10.6% of liquid bars, DIV/UPP ~6%. "
                   "A mark this common is context; only TURN (0.74%), UP4 (1.42%) and REV (1.64%) "
                   "are rare. High base rates are recorded, not corrected for.",
        no_sequence_cells="the nine are mutually exclusive, and the descriptive succession pass "
                          "showed every A->B lift at lag 1-2 only, collapsing to 1.00 from lag 3 "
                          "(DIV->RE2 = 0.00, TURN->TURN = 0.00) — shared bar windows, arithmetic.",
        deviations=["price floor 21 not 5 (follows CONTROL_KEYS v2)",
                    "window is the control artifact's 2021-09-07..2026-09-03",
                    "no 1-in-N sampling (ADJACENT_TURN_V1 lesson + cooldown estimand)"],
        why_placebo="UDN_CONFLICT_V1's 'directional' verdict died when size-matched placebos spanned "
                    "-0.07..+0.44 at 18k trades while the claim rested on -0.07. Cells here differ "
                    "~14x in size (VUP vs TURN), so the band is built in, not bolted on.",
        why_control_restricted="in that same family 'buy every bar' scored +0.34 with DSR 0.9987 "
                               "because the control covered more tickers than the cells.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED",
               sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), known_limitations=lim,
               provenance=["USER_APPROVED_2026-09-21 (\"gaushvi\" to the 5-line plan)",
                           "USER-SUPPLIED DESIGN — Pine 260921 PRICE x VOLUME Multi Signal",
                           "PRE_OUTCOME", "PRE_PATHSIM"],
               question=rep["signal"]["question"], signal=rep["signal"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}",
               control=f"CONTROL_KEYS v2 ({rep['control']['run_id']}) restricted to the family's "
                       f"tickers; per entry date the control-day median (>= {O.MIN_CONTROL} others)",
               placebo=f"{NPLACEBO} size-matched draws from the control trades, day-clustered "
                       f"identically; a cell must sit outside that band (p < 0.05) in BOTH windows",
               estimand="direct sacred edge_replay._pathsim, engine cooldown INCLUDED, entry D+1 open",
               oos=OOS, gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="one registered cell per shape"),
               cells=[dict(claim_id=k, rule=r, direction="none registered") for k, r in SIGNALS],
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], census=rep["census"],
               stop_rule="ONE outcome run. A cell passes only if the book gates classify it "
                         "BUILD_CANDIDATE or VETO_CANDIDATE *and* it clears the size-matched placebo "
                         "band in BOTH windows. Nothing passes -> NULL, family closes. No threshold, "
                         "window, price-bucket or timeframe search follows, and no failed cell is "
                         "re-read as a different shape than the one registered here.")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")
    json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
             slip=slip, n_placebo=NPLACEBO, rng_seed=RNG_SEED,
             code=dict(pv_multi_family=_dig(__file__), edge_replay=_dig(os.path.join(HERE, "edge_replay.py")),
                       control_keys=_dig(os.path.join(HERE, "control_keys.py")), ovd_outcome_v1=_dig(O.__file__),
                       ovd_outcome_direct_v1=_dig(OD.__file__)),
             outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


def _day_median_edge(e: pd.DataFrame) -> float:
    """The registered statistic: median over per-DAY medians of edge. Same order as O.day_stats."""
    if not len(e):
        return float("nan")
    return float(np.median(e.groupby("date_in")["edge"].median().to_numpy()))


def placebo_band(ctr: pd.DataFrame, n_obs: int, lo: str, hi: str, rng) -> np.ndarray:
    """Size-matched draws from the CONTROL trades — signal-free by construction, scored exactly like
    a cell. Answers 'what does a cell THIS BIG produce when nothing is there?'."""
    w = ctr[(ctr["date_in"] >= lo) & (ctr["date_in"] <= hi)]
    w = w[w["edge"].notna()]
    if len(w) < 2 or n_obs < 1:
        return np.array([])
    take = min(n_obs, len(w))
    idx = np.arange(len(w))
    out = np.empty(NPLACEBO)
    for i in range(NPLACEBO):
        out[i] = _day_median_edge(w.iloc[rng.choice(idx, size=take, replace=take > len(w))])
    return out[~np.isnan(out)]


def outcome(log=print) -> dict:
    sp = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(sp):
        raise HardStop("not sealed")
    s = json.load(open(sp))
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); reg = json.load(open(rp))
    if _dig(rp) != s["registry_sha256_16"] or len(reg["cells"]) != s["k"]:
        raise HardStop("registry digest / k mismatch")
    run_x = os.path.join(FAMILY_DIR, "runs", s["x_run"])
    if _dig(os.path.join(run_x, "X.parquet")) != s["x_sha256_16"] or not RunSpace.is_current(run_x, "X.parquet"):
        raise HardStop("X mismatch")
    cur = B.canonical_current()
    if cur["derived_sha256_16"] != s["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical changed since sealing")
    man = CK.current()
    if man["sha256_16"] != s["control"]["sha256_16"]:
        raise HardStop("control artifact changed since sealing")
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "pv_multi")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                              seal_registry=s["registry_sha256_16"]))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")

    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    tick = set(X["ticker"].unique())
    ctl = CK.load(man); ctl["session"] = ctl["session"].astype(str).str[:10]
    ctl = ctl[ctl["ticker"].isin(tick) & (ctl["session"] >= OOS["MINE"][0]) & (ctl["session"] <= OOS["VERIFY"][1])]
    ctl = ctl.reset_index(drop=True)
    log(f"  control keys on the family's tickers: {len(ctl):,} · {ctl['ticker'].nunique():,} tickers")
    frames = OD.load_frames(cur["derived_parquet"], pd.concat([X["ticker"], ctl["ticker"]]).unique())
    ctr, ctl_cons = OD.direct_trades(ps, frames, ctl[["ticker", "session"]])
    ctr = ctr[ctr["ret"].notna()].reset_index(drop=True)
    cmed = ctr.groupby("date_in")["ret"].median(); ccnt = ctr.groupby("date_in")["ret"].size()
    loo, noth = O.loo_control(ctr, "ret")
    ctr["edge"] = np.where(noth >= O.MIN_CONTROL, ctr["ret"].to_numpy(float) - loo, np.nan)
    log(f"  control trades {len(ctr):,} on {ctr['date_in'].nunique():,} dates")

    rng = np.random.default_rng(RNG_SEED)
    results, mine_series, cons_all = [], {}, dict(control=ctl_cons)
    for c in reg["cells"]:
        cid = c["claim_id"]
        m = cell_mask(X, cid)
        tr, cons = OD.direct_trades(ps, frames, X.loc[m, ["ticker", "session"]])
        cons_all[cid] = cons
        obs = tr[tr["ret"].notna()].copy()
        if len(obs):
            obs["control"] = obs["date_in"].map(cmed).to_numpy(float)
            obs["n_others"] = obs["date_in"].map(ccnt).fillna(0).to_numpy(int)
            obs["edge"] = np.where(obs["n_others"] >= O.MIN_CONTROL,
                                   obs["ret"].to_numpy(float) - obs["control"].to_numpy(float), np.nan)
        else:
            obs = obs.assign(control=np.nan, n_others=0, edge=np.nan)
        mine = _stats_window(obs, *OOS["MINE"]); ver = _stats_window(obs, *OOS["VERIFY"])
        pl = {}
        for w, (lo, hi), st in (("mine", OOS["MINE"], mine), ("verify", OOS["VERIFY"], ver)):
            band = placebo_band(ctr, int(st["n_obs"]), lo, hi, rng)
            real = st["median_edge"]
            # NEVER `(p or 1)`: p == 0.0 is falsy and would read as "no evidence" (ladder_family:334)
            p = None if (real is None or not len(band)) else round(float((np.abs(band) >= abs(real)).mean()), 4)
            pl[f"{w}_placebo_p"] = p
            pl[f"{w}_placebo_sd"] = None if not len(band) else round(float(band.std()), 3)
            pl[f"{w}_placebo_lo"] = None if not len(band) else round(float(np.percentile(band, 2.5)), 3)
            pl[f"{w}_placebo_hi"] = None if not len(band) else round(float(np.percentile(band, 97.5)), 3)
        results.append(dict(claim_id=cid, rule=c["rule"], **{k: v for k, v in cons.items() if k != "identity"},
                            mine={k: v for k, v in mine.items() if k != "day_series"},
                            verify={k: v for k, v in ver.items() if k != "day_series"}, placebo=pl))
        mine_series[cid] = mine["day_series"]
        log(f"  {cid:5s} sig {cons['signal_true_n']:>7,} -> trades {cons['direct_selected_n']:>6,} "
            f"| MINE med {mine['median_edge']} dw {mine['day_win']} | VERIFY med {ver['median_edge']} "
            f"dw {ver['day_win']} | placebo p {pl['mine_placebo_p']}/{pl['verify_placebo_p']}", flush=True)
    del frames; gc.collect()

    from overfit_stats import dsr, sharpe
    kids = [r["claim_id"] for r in results]
    trial = [sharpe(mine_series[k].to_numpy()) if len(mine_series[k]) >= 2 else 0.0 for k in kids]
    from breadth_family import classify as book_classify
    for r in results:
        srs = mine_series[r["claim_id"]].to_numpy(float)
        if len(srs) >= 3:
            dp = dsr(srs, trial, n_trials=K_EXPECTED); dn = dsr(-srs, [-x for x in trial], n_trials=K_EXPECTED)
            r.update(dsr=dp["dsr"], sr=dp["sr"], sr_star=dp["sr_star"], dsr_neg=dn["dsr"])
        else:
            r.update(dsr=None, sr=None, sr_star=None, dsr_neg=None)
        book = book_classify(r["mine"], r["verify"], r["dsr"], r["dsr_neg"])
        pm, pv = r["placebo"]["mine_placebo_p"], r["placebo"]["verify_placebo_p"]
        clears = pm is not None and pv is not None and pm < GATES["placebo"]["p_lt"] and pv < GATES["placebo"]["p_lt"]
        r["book_classification"] = book
        r["clears_placebo"] = bool(clears)
        r["classification"] = book if (book in ("BUILD_CANDIDATE", "VETO_CANDIDATE") and clears) else (
            "NULL_PLACEBO_NOT_CLEARED" if book in ("BUILD_CANDIDATE", "VETO_CANDIDATE") else book)
    if sorted(kids) != sorted(c["claim_id"] for c in reg["cells"]):
        raise HardStop("result table != registry")
    run.write_atomic("selection_conservation.json", cons_all)
    run.write_atomic("results.json", results)
    cls_census = {}
    for r in results:
        cls_census[r["classification"]] = cls_census.get(r["classification"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED,
                oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                control=dict(trades=int(len(ctr)), dates=int(ctr["date_in"].nunique()), run_id=man["run_id"]),
                outcome_access_count=led["outcome_access_count"], classification_census=cls_census,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id,
                   seal_registry_sha256_16=s["registry_sha256_16"],
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")),
                   outcome_access_count=led["outcome_access_count"], pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                   classification_census=cls_census, status="COMPLETE — family SEARCH-EXPOSED",
                   rule="every later idea born from these outcomes is POST_EXPOSURE_HYPOTHESIS; "
                        "no threshold / window / price-bucket / TF search follows"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"classification: {cls_census}")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    if cmd == "build":
        build()
    elif cmd == "seal":
        s = seal(); print(json.dumps({k: s[k] for k in ("sealed_at", "registry_sha256_16", "k", "x_run", "x_sha256_16")}, indent=1))
    elif cmd == "outcome":
        outcome()
    else:
        raise SystemExit("build | seal | outcome")
