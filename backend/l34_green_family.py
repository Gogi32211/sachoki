"""L34_GREEN_V1 — inside L34 ∧ 💪RS, does the CANDLE COLOUR matter? User-approved 2026-09-11. k = 1.

  The user asked for green: "სანთელი მწვანე და L34+💪". This is one registered claim.

  ⚠ STATUS — SUBGROUP OF A FAILED CELL. PAIR_CONFLUENCE_V1 tested L34 × 💪 and its interaction
  failed: −0.443 in MINE (p 0.302) and +0.330 in VERIFY (p 0.512), sign not held. Splitting that
  same cell by candle colour is subgroup search, which is how a dead cell gets resurrected by
  accident. Declared before sealing: a PASS here is a CANDIDATE needing its own forward window,
  never a BUILD. The user pointed at it from the chart, which is a-priori on their side; I had
  already seen PAIR_CONFLUENCE's outcome, which is not.

  ⚠ THE STATISTIC IS AGAIN THE INTERACTION, and here it matters even more than it did for RS.
  GREEN is 50.44 % of ALL liquid sessions, and the book's central law
  ([[project_what_actually_works]]) is that absorbed WEAKNESS pays and strength does not — so the
  colour has a large universal main effect of its own. `GREEN − RED inside L34∧RS` would measure
  that law, not this setup. The registered object is

      INTERACTION = [edge(A∧GREEN) − edge(A∧RED)] − [edge(¬A∧GREEN) − edge(¬A∧RED)],  A = L34 ∧ rs

  i.e. does colour behave DIFFERENTLY on an L34∧💪 bar than it behaves everywhere else?

  ⚠ REGISTERED DIRECTION IS THE BOOK'S, NOT THE USER'S QUESTION — and this is deliberate.
  [[project_l34_red_triple]] found red-L34 good and green-L34 a trap (in the 🟢REV context), and the
  strength-fade law points the same way. So the registered direction is NEGATIVE: red should beat
  green by more on L34∧💪 bars than elsewhere. Registering the user's hope instead would be
  pretending my prior is something it is not. The placebo is TWO-SIDED, so a green-favouring result
  is fully detectable: if the interaction comes out POSITIVE, consistent across both windows and
  outside the placebo in both, it is recorded as REFUTES_REGISTERED_DIRECTION — a refutation of the
  book's own prior, which is a more interesting outcome than a pass, and stands as a candidate
  requiring a clean forward window.

  KNOWN BEFORE SEALING:
    · colour = close > open (GREEN) / close < open (RED), taken from the CANONICAL frame, not from
      lvx_signals. They agree on 99.979 % of the 193,142 L34 rows where both exist; the 40
      disagreements are all on the DOJI boundary, where lbal_build falls back to a 15m open/close.
    · DOJI is EXCLUDED (0.49 % of sessions). lbal's own ★ gate already refuses to mark a doji.
    · cells: A∧GREEN 35,844 MINE / 21,783 VERIFY · A∧RED 11,741 / 7,142 · ¬A∧GREEN 598,902 /
      339,496 · ¬A∧RED 613,113 / 337,132. Nothing thin.
    · 💪 is anatomy_signals.rs, the ▽△ row's own badge — the same flag PAIR_CONFLUENCE_V1 used, so
      this subgroup sits strictly inside that family's A cell and cannot drift from it.

  Instrument: NO sampling — every qualifying session, cooldown left in place
  ([[feedback-pathsim-cooldown-estimand]]); one universe for all four cells; CONTROL_KEYS v2
  restricted to the family's tickers; DAY-CLUSTERED statistic; 500 size-matched label shuffles;
  MINE 2021-09-07..2024-12-31 decides, VERIFY 2025-01-01..2026-09-03 replicates; one outcome run.
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
from breadth_family import GATES, _stats_window                          # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/L34_GREEN_V1"
FAMILY = "L34_GREEN_V1"
DATA = os.path.join(os.path.dirname(HERE), "data")
ANAT = os.path.join(DATA, "anatomy_signals.parquet")
LVX = os.path.join(DATA, "lvx_signals.parquet")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED, NSHUF, RNG_SEED = 1, 500, 20260911
CELLS = ["A_GREEN", "A_RED", "N_GREEN", "N_RED"]
REGISTERED_SIGN = -1          # the book: red beats green MORE on L34∧💪 than elsewhere


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    for p in (ANAT, LVX):
        if not os.path.exists(p):
            raise HardStop(f"missing {p}")
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("LG_%Y%m%dT%H%M%SZ", time.gmtime()))

    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "open", "close", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    liq = px[(px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
             & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1])].copy()
    del px; gc.collect()
    liq["green"] = liq["close"].to_numpy() > liq["open"].to_numpy()
    liq["doji"] = liq["close"].to_numpy() == liq["open"].to_numpy()
    n_doji = int(liq["doji"].sum())
    liq = liq.loc[~liq["doji"], ["ticker", "session", "green"]].drop_duplicates(subset=["ticker", "session"])
    log(f"  liquid non-doji {len(liq):,} (doji dropped {n_doji:,})")

    ana = pd.read_parquet(ANAT, columns=["ticker", "date", "rs"]).rename(columns={"date": "session"})
    X = liq.merge(ana, on=["ticker", "session"], how="inner")           # anatomy present = the universe
    X["rs"] = X["rs"].astype(bool)
    lvx = pd.read_parquet(LVX, columns=["ticker", "date", "fam"]).rename(columns={"date": "session"})
    l34 = lvx.loc[lvx["fam"] == "L34", ["ticker", "session"]].drop_duplicates()
    l34["L34"] = True
    X = X.merge(l34, on=["ticker", "session"], how="left")
    X["L34"] = X["L34"].fillna(False).infer_objects(copy=False).astype(bool)

    A = (X["L34"] & X["rs"]).to_numpy()
    G = X["green"].to_numpy()
    X["cell"] = np.where(A & G, "A_GREEN", np.where(A & ~G, "A_RED",
                         np.where(~A & G, "N_GREEN", "N_RED")))
    X = X[["ticker", "session", "cell"]].sort_values(["ticker", "session"],
                                                     kind="mergesort").reset_index(drop=True)
    census = {}
    for c in CELLS:
        m = (X["cell"] == c).to_numpy()
        census[c] = dict(n=int(m.sum()), tickers=int(X.loc[m, "ticker"].nunique()),
                         mine=int((m & (X["session"] <= OOS["MINE"][1]).to_numpy()).sum()),
                         verify=int((m & (X["session"] >= OOS["VERIFY"][0]).to_numpy()).sum()))
        log(f"  {c:9s} {census[c]['n']:>9,}  (MINE {census[c]['mine']:,} / "
            f"VERIFY {census[c]['verify']:,})  tickers {census[c]['tickers']:,}")
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    log(f"  X rows {len(X):,}")

    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, n_shuffles=NSHUF, rng_seed=RNG_SEED,
               sampling="NONE — every qualifying session; path-sim cooldown left in place",
               signal=dict(name="L34_GREEN", cells=CELLS,
                           A="L34 (lvx_signals.fam=='L34') AND rs (anatomy_signals.rs) — the same "
                             "cell PAIR_CONFLUENCE_V1's L34_x_RS BOTH used",
                           B="candle colour from the CANONICAL frame: close>open GREEN, close<open "
                             "RED, doji EXCLUDED",
                           registered_statistic="INTERACTION = [edge(A_GREEN) - edge(A_RED)] - "
                                                "[edge(N_GREEN) - edge(N_RED)], day-clustered",
                           registered_direction="NEGATIVE (red beats green MORE on L34∧💪 than "
                                                "elsewhere) — the BOOK's prior, not the user's "
                                                "question; a consistent significant POSITIVE is "
                                                "recorded as REFUTES_REGISTERED_DIRECTION",
                           status="SUBGROUP OF A FAILED CELL — PAIR_CONFLUENCE_V1's L34_x_RS "
                                  "interaction was -0.443 (p 0.302) MINE / +0.330 (p 0.512) VERIFY",
                           doji_dropped=n_doji,
                           colour_parity="canonical OHLC vs lvx_signals.colour agree on 99.979% of "
                                         "the 193,142 L34 rows where both exist; all 40 "
                                         "disagreements sit on the doji boundary",
                           sources={p: _dig(p) for p in (ANAT, LVX)}),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               census=census, x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "LG_*")), reverse=True):
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
        cell_counts=rep["census"],
        multiplicity="SUBGROUP SEARCH. PAIR_CONFLUENCE_V1 (k=3, same session, same user arc) tested "
                     "L34 × 💪 and failed. This re-cuts that cell by candle colour. A PASS is a "
                     "CANDIDATE needing a clean forward window, never a BUILD. Running k for this "
                     "arc is 3 + 1 = 4.",
        why_interaction="GREEN is 50.44% of all liquid sessions and the book's central law is that "
                        "strength underperforms (project_what_actually_works). `GREEN - RED inside "
                        "L34∧RS` would restate that law rather than test this setup, so the "
                        "registered object is the 2x2 interaction against colour's own behaviour "
                        "everywhere else.",
        direction_is_the_books="Registered NEGATIVE from project_l34_red_triple (red-L34 good, "
                               "green-L34 a trap, established in the 🟢REV context) plus the "
                               "strength-fade law. The user asked about GREEN; registering their "
                               "hope would misstate my prior. The placebo is two-sided, so a "
                               "green-favouring result is detectable and is recorded as "
                               "REFUTES_REGISTERED_DIRECTION rather than silently discarded.",
        prior_scope_caveat="project_l34_red_triple's red/green law was established in the 🟢REV "
                           "context, not on every L34 bar. Carrying it here is an extrapolation, "
                           "and that is exactly why the result can refute it.",
        doji="EXCLUDED (0.49% of sessions). lbal_build's own ★ gate refuses to mark a doji.",
        level_caveat="Cell LEVELS are not comparable across windows: PAIR_CONFLUENCE_V1 measured "
                     "population_vs_control at -0.407 in MINE and +0.389 in VERIFY on this same "
                     "universe. The interaction cancels that offset; the printed levels do not.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED",
               known_limitations=lim, sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-11 (\"ki\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "USER ASKED FOR THE GREEN CANDLE ON L34+💪"],
               signal=rep["signal"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}, "
                        f"anatomy_signals present, doji excluded",
               control=f"CONTROL_KEYS v2 ({rep['control']['run_id']}) restricted to this family's "
                       f"tickers; control-day median needs >= {O.MIN_CONTROL} trades",
               estimand="direct sacred edge_replay._pathsim, engine cooldown INCLUDED",
               oos=OOS, gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="one interaction test"),
               cells=[dict(claim_id="L34_RS_COLOUR", rule="2x2 on (L34 and rs) x (green vs red)",
                           expect="decrease (red favoured)")],
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], census=rep["census"],
               stop_rule="PASS only if the INTERACTION keeps its registered NEGATIVE sign in MINE "
                         "and VERIFY AND sits outside the size-matched placebo (p < 0.05) in BOTH. "
                         "A consistent significant POSITIVE is REFUTES_REGISTERED_DIRECTION, a "
                         "candidate, not a pass. One outcome run; no re-cut.")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")
    json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
             slip=slip, n_shuffles=NSHUF, rng_seed=RNG_SEED, registered_sign=REGISTERED_SIGN,
             code=dict(l34_green_family=_dig(__file__),
                       pair_confluence_family=_dig(os.path.join(HERE, "pair_confluence_family.py")),
                       anatomy_build=_dig(os.path.join(HERE, "anatomy_build.py")),
                       lbal_build=_dig(os.path.join(HERE, "lbal_build.py")),
                       edge_replay=_dig(os.path.join(HERE, "edge_replay.py"))),
             outcome_access_count=0, rule="registry frozen; outcome is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


def _cell_edges(sub: pd.DataFrame, lo: str, hi: str) -> dict:
    """DAY-CLUSTERED per cell: per entry day the cell's median trade minus that day's control
    median, then the median over DAYS."""
    w = sub[(sub["date_in"] >= lo) & (sub["date_in"] <= hi)]
    if not len(w):
        return {c: (np.nan, 0) for c in CELLS}
    byday = (w.groupby(["cell", "date_in"], sort=False)
               .agg(m=("ret", "median"), c=("cmed", "first")))
    byday["e"] = byday["m"] - byday["c"]
    out = {}
    lvl0 = byday.index.get_level_values(0)
    for c in CELLS:
        if c in lvl0:
            e = byday.loc[c, "e"].to_numpy(float)
            out[c] = (float(np.median(e)) if len(e) else np.nan, int(len(e)))
        else:
            out[c] = (np.nan, 0)
    return out


def _interaction(ed: dict):
    v = [ed[c][0] for c in CELLS]
    if any(np.isnan(x) for x in v):
        return None
    return (v[0] - v[1]) - (v[2] - v[3])


def outcome(log=print) -> dict:
    sp = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(sp):
        raise HardStop("not sealed")
    s = json.load(open(sp))
    if _dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")) != s["registry_sha256_16"]:
        raise HardStop("registry digest mismatch")
    run_x = os.path.join(FAMILY_DIR, "runs", s["x_run"])
    if _dig(os.path.join(run_x, "X.parquet")) != s["x_sha256_16"]:
        raise HardStop("X mismatch")
    cur = B.canonical_current()
    if cur["derived_sha256_16"] != s["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical changed")
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "l34green")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")

    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    ctl = CK.load(man); ctl["session"] = ctl["session"].astype(str).str[:10]
    ctl = ctl[(ctl["session"] >= OOS["MINE"][0]) & (ctl["session"] <= OOS["VERIFY"][1])]
    ctl = ctl[ctl["ticker"].isin(set(X["ticker"].unique()))].reset_index(drop=True)
    frames = OD.load_frames(cur["derived_parquet"],
                            pd.concat([X["ticker"], ctl["ticker"]]).unique())
    ctr, _ = OD.direct_trades(ps, frames, ctl[["ticker", "session"]]); ctr = ctr[ctr["ret"].notna()]
    med = ctr.groupby("date_in")["ret"].median(); cnt = ctr.groupby("date_in")["ret"].size()
    tr, _ = OD.direct_trades(ps, frames, X[["ticker", "session"]])
    tr = tr.merge(X, on=["ticker", "session"], how="left")
    sub = tr[tr["ret"].notna()].copy()
    sub["cmed"] = sub["date_in"].map(med).to_numpy(float)
    sub["cn"] = sub["date_in"].map(cnt).fillna(0).to_numpy(int)
    sub = sub[sub["cn"] >= O.MIN_CONTROL].copy()
    del frames; gc.collect()
    log(f"  sessions {len(X):,} · usable trades {len(sub):,} · control {len(ctr):,}\n")

    real, days, desc = {}, {}, {}
    for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
        ed = _cell_edges(sub, lo, hi)
        it = _interaction(ed)
        real[w] = dict(**{c.lower(): (None if np.isnan(ed[c][0]) else round(ed[c][0], 3))
                          for c in CELLS},
                       interaction=None if it is None else round(it, 3))
        days[w] = {c.lower() + "_days": ed[c][1] for c in CELLS}
        ag, ar = ed["A_GREEN"][0], ed["A_RED"][0]
        ng, nr = ed["N_GREEN"][0], ed["N_RED"][0]
        desc[w] = dict(green_minus_red_inside=None if (np.isnan(ag) or np.isnan(ar)) else round(ag - ar, 3),
                       green_minus_red_elsewhere=None if (np.isnan(ng) or np.isnan(nr)) else round(ng - nr, 3),
                       setup_vs_control=None if np.isnan(ag) else round(ag, 3))

    base = sub["cell"].to_numpy().copy()
    rng = np.random.default_rng(RNG_SEED)
    pl = {w: [] for w in ("mine", "verify")}
    for _ in range(NSHUF):
        sub["cell"] = rng.permutation(base)
        for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
            pl[w].append(_interaction(_cell_edges(sub, lo, hi)))
    sub["cell"] = base
    pv = {}
    for w in ("mine", "verify"):
        arr = np.asarray([x for x in pl[w] if x is not None], float)
        r0 = real[w]["interaction"]
        pv[f"{w}_p"] = (None if (r0 is None or not len(arr))
                        else round(float((np.abs(arr) >= abs(r0)).mean()), 4))
        pv[f"{w}_null_sd"] = None if not len(arr) else round(float(arr.std()), 3)
    popd = {w: _stats_window(sub.assign(edge=sub["ret"] - sub["cmed"]), lo, hi)["median_edge"]
            for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"]))}

    im, iv = real["mine"]["interaction"], real["verify"]["interaction"]
    pm, pvf = pv["mine_p"], pv["verify_p"]
    same = im is not None and iv is not None and im * iv > 0
    held = bool(same and np.sign(im) == REGISTERED_SIGN)
    sig_both = pm is not None and pvf is not None and pm < 0.05 and pvf < 0.05
    passes = bool(held and sig_both)
    refutes = bool(same and not held and sig_both)

    for w in ("mine", "verify"):
        r = real[w]
        log(f"  {w.upper():6s} A_GREEN {r['a_green']} · A_RED {r['a_red']} · "
            f"N_GREEN {r['n_green']} · N_RED {r['n_red']}")
        log(f"         INTERACTION {r['interaction']} (p {pv[w+'_p']}, null sd {pv[w+'_null_sd']})"
            f" · days {days[w]}")
        log(f"         [descriptive] {desc[w]}")
    log(f"\n  registered direction NEGATIVE (red favoured) · same_sign {same} · held {held} "
        f"· significant in both {sig_both}")
    log(f"  pop_vs_control {popd}")
    log(f"  PASSES {passes} · REFUTES_REGISTERED_DIRECTION {refutes}")

    res = dict(claim="L34_RS_COLOUR", status="SUBGROUP OF PAIR_CONFLUENCE_V1's FAILED L34_x_RS CELL",
               registered_direction="NEGATIVE (red favoured)", real=real, days=days, placebo=pv,
               descriptive=desc, population_vs_control=popd, same_sign=same,
               direction_held=held, significant_in_both=sig_both,
               PASSES=passes, REFUTES_REGISTERED_DIRECTION=refutes)
    run.write_atomic("results.json", res)
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"],
                k=K_EXPECTED, oos=OOS, outcome_access_count=led["outcome_access_count"],
                PASSES=passes, REFUTES_REGISTERED_DIRECTION=refutes,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items()
                                       if k in ("sealed_at", "registry_sha256_16", "k", "x_run")}, indent=1)),
     "outcome": outcome}[cmd]()
