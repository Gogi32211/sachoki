"""OPENING_VOLUME_DYNAMICS — POST-EXPOSURE PATHSIM SEMANTICS RECONCILIATION (execution correction, not a new run design).

Outcome is already exposed (first execution OUT_20260904T140250Z, immutable). This module:
  1. recovers the PRE-OUTCOME authority on trade selection from the SEALED artifacts (quotes + digests) — never
     from post-outcome runner code; if no explicit cooldown-neutralisation authority exists, the direct sacred
     _pathsim semantics (stateful 5-bar same-ticker cooldown included) are authoritative;
  2. classifies the first run accurately (EXECUTION_SEMANTICS_DEVIATION if mod-5 differs) — nothing deleted;
  3. executes every registered cell through the UNMODIFIED sacred _pathsim with its ACTUAL registered signal mask
     in ONE direct call (no mod-N partitions, no cooldown bypass, no local simulator), same frozen SEAL_V2_1 /
     registry k=300 / X / _pathsim source / trail / maxh / slip / entry timing / controls / multiplicity;
  4. recomputes the frozen primary statistic on the direct cooldown-selected trade population only: control =
     leave-one-out median of the OTHER direct trades of the same family on the same entry date on which the cell's
     feature(s) are evaluable (the union of the feature's bin-cells' direct trades; for a state cell the FALSE side is
     run as a control-only mask), self excluded, >= 20 others; trade edge -> entry-day median -> cell median;
  5. compares MOD5 vs DIRECT per cell (no optimisation, no selection of the better-looking version).
Label of the result: POST_EXPOSURE_EXECUTION_CORRECTION — not independent confirmation.
outcome_access_count increments (never resets). No V3, no thresholds, no new search.
"""
from __future__ import annotations
import os, sys, re, json, time, hashlib, glob                           # noqa: E402
import numpy as np                                                      # noqa: E402
import pandas as pd                                                     # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import HardStop, RunSpace                                # noqa: E402
import ovd_seal_v2 as S2                                                # noqa: E402
import ovd_registry_v2 as R2                                            # noqa: E402
import ovd_outcome_v1 as O                                              # noqa: E402  (cell definitions, statistics, gates — reused unchanged)

FAMILY_DIR = O.FAMILY_DIR
CANON_DIR = O.CANON_DIR
FIRST_RUN = "OUT_20260904T140250Z"
SEALED_ARTIFACTS = ["FEATURE_SPEC_V1.json", "SEARCH_REGISTRY_V1.json", "SEAL.json", "FEATURE_SPEC_V2.json", "SEARCH_REGISTRY_V2.json",
                    "SEAL_V2.json", "FEATURE_SPEC_V2_1.json", "SEARCH_REGISTRY_V2_1.json", "SEAL_V2_1.json", "PRE_OUTCOME_DESIGN_AMENDMENT_V2.json",
                    "PRE_OUTCOME_RESEAL_V2_1.json", "discovery_phase1.json"]
PATTERNS = [r"cooldown", r"cool-down", r"i\s*-\s*last", r"independent", r"observation[- ]level", r"every valid observation", r"mod[- ]?5",
            r"partition", r"neutrali", r"suppress", r"each observation", r"per[- ]observation", r"own _pathsim outcome", r"non-null _pathsim outcome"]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


# the staggered-partition idiom of the first run (bar index modulo N) must not appear in the direct runner
PARTITION_IDIOM = re.compile(r"\bpos\s*%\s*\d+\b|arange\([^)]*\)\s*%\s*\d+|\)\s*%\s*\d+\s*==")


# ── 1 · pre-outcome authority recovery ─────────────────────────────────────────────────────
def recover_authority() -> dict:
    hits = []
    for name in SEALED_ARTIFACTS:
        p = os.path.join(FAMILY_DIR, name)
        if not os.path.exists(p):
            continue
        txt = open(p, encoding="utf-8").read()
        for pat in PATTERNS:
            for m in re.finditer(pat, txt, re.I):
                a, b = max(0, m.start() - 160), min(len(txt), m.end() + 160)
                hits.append(dict(artifact=name, sha256_16=_dig(p), pattern=pat, quote=txt[a:b].replace("\n", " ")))
    explicit_neutralisation = [h for h in hits if re.search(r"cooldown|cool-down|i\s*-\s*last|mod[- ]?5|neutrali|suppress", h["pattern"], re.I)]
    verdict = ("B_DIRECT_SACRED_PATHSIM_WITH_COOLDOWN" if not explicit_neutralisation else "REVIEW")
    reg1 = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V1.json")))
    binding = dict(entry=reg1["entry"], primary_statistic=reg1["primary_statistic"], control_universe=reg1["same_day_control"]["control_universe"],
                   algorithm=reg1["same_day_control"]["algorithm"], registry_sha256_16=_dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V1.json")))
    return dict(verdict=verdict, rule="no sealed pre-outcome artifact registers cooldown neutralisation or observation-level independent path "
                                     "outcomes; the sealed wording is 'mask on session D -> _pathsim entry at D+1 open' with the sacred engine's "
                                     "exact existing semantics -> direct signal-mask semantics (cooldown included) are authoritative",
                v1_registry_binding=binding, explicit_neutralisation_hits=explicit_neutralisation, all_hits=hits,
                first_run_classification="EXPOSED_EXECUTION_WITH_PATHSIM_SELECTION_SEMANTICS_DEVIATION (mod-5 union != direct call under the 5-bar cooldown)",
                recovered_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))


# ── 3 · direct execution ───────────────────────────────────────────────────────────────────
def load_frames(canon_parquet: str, tickers) -> dict:
    df = pd.read_parquet(canon_parquet, columns=["ticker", "session_date", "open", "high", "low", "close", "atr_14"])
    df = df[df["ticker"].isin(set(tickers))].rename(columns={"session_date": "date"})
    df["date"] = df["date"].astype(str)
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    frames = {}
    for tk, g in df.groupby("ticker", sort=False):
        g = g.reset_index(drop=True); g["CELL"] = False
        frames[tk] = g
    return frames


def direct_trades(ps, frames: dict, keys: pd.DataFrame, maxh: int = 60) -> tuple[pd.DataFrame, dict]:
    """ONE sacred _pathsim call per mask over the full bar history of every ticker that carries a signal.
    keys: DataFrame(ticker, session) of TRUE signal rows. Returns trades + exact selection conservation."""
    by_tk = keys.groupby("ticker")["session"].apply(set).to_dict()
    grp = {}
    for tk, sess in by_tk.items():
        g = frames[tk]
        g["CELL"] = g["date"].isin(sess).to_numpy()
        grp[tk] = g
    tr = ps(grp, "CELL", O.PARAMS["mode"], O.PARAMS["stop"], O.PARAMS["target"], O.PARAMS["trail_fallback"], maxh, slip=None, atr_k=O.PARAMS["atr_k"])
    no_next = 0; no_open = 0; sig_pos = {}
    for tk, sess in by_tk.items():
        g = frames[tk]; d = g["date"].to_numpy(); o = g["open"].to_numpy(float); n = len(g)
        idx = np.where(g["CELL"].to_numpy())[0]
        no_next += int((idx + 1 >= n).sum())
        nx = idx[idx + 1 < n]
        no_open += int((~(o[nx + 1] > 0)).sum())
        sig_pos[tk] = dict(zip(d, range(n)))
        g["CELL"] = False
    if len(tr):
        nxt = {tk: dict(zip(frames[tk]["date"].to_numpy()[1:], frames[tk]["date"].to_numpy()[:-1])) for tk in by_tk}
        tr["session"] = [nxt[t][d] for t, d in zip(tr["ticker"], tr["date_in"])]
        pos = np.array([sig_pos[t][s] for t, s in zip(tr["ticker"], tr["session"])])
        n_tk = tr["ticker"].map({tk: len(frames[tk]) for tk in by_tk}).to_numpy()
        last = tr["ticker"].map({tk: frames[tk]["date"].iloc[-1] for tk in by_tk}).to_numpy()
        tr["censored"] = ((pos + 1 + maxh) > n_tk) & (tr["date_out"].to_numpy() == last)
        tr["ret"] = tr["ret"].astype(float) * 100.0
        # engine-output consistency: taken signals on one ticker are >= 5 bars apart (cooldown honoured)
        gaps = pd.Series(pos).groupby(tr["ticker"].to_numpy()).diff().dropna()
        if (gaps < 5).any():
            raise HardStop("direct trades violate the engine's own cooldown — impossible")
    else:
        tr = pd.DataFrame(columns=["ticker", "ret", "yr", "date_in", "date_out", "mae", "mfe", "hold", "risk", "session", "censored"])
    signal_n = int(len(keys)); taken = int(len(tr))
    cooldown = signal_n - taken - no_next - no_open
    if cooldown < 0:
        raise HardStop(f"selection conservation broken: signal {signal_n} taken {taken} no_next {no_next} no_open {no_open}")
    cons = dict(signal_true_n=signal_n, direct_selected_n=taken, cooldown_suppressed_n=int(cooldown), NO_NEXT_SESSION=no_next, NO_ENTRY_OPEN=no_open,
                defined_n=int(tr["ret"].notna().sum()) if len(tr) else 0, right_censored_n=int(tr["censored"].sum()) if len(tr) else 0,
                identity="signal_true = direct_selected + cooldown_suppressed + NO_NEXT_SESSION + NO_ENTRY_OPEN")
    return tr, cons


def run(log=print) -> dict:
    seal = S2.execution_authority()
    if seal["amendment"] != "OPENING_VOLUME_DYNAMICS_PRE_OUTCOME_RESEAL_V2_1" or hashlib.sha256(open(os.path.join(FAMILY_DIR, "SEAL_V2_1.json"), "rb").read()).hexdigest() != O.SEAL_V2_1_SHA256:
        raise HardStop("SEAL_V2_1 identity mismatch")
    first = os.path.join(FAMILY_DIR, "runs", FIRST_RUN)
    if not RunSpace.is_current(first, "results_300.parquet"):
        raise HardStop("the first exposed run is not CURRENT — it must be preserved as immutable provenance")
    led = O.ledger_read()
    if led["outcome_access_count"] < 1:
        raise HardStop("outcome not yet exposed — this module is a POST-exposure correction only")
    reg_p = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V2_1.json")
    if _dig(reg_p) != seal["registry_sha256_16"]:
        raise HardStop("registry digest mismatch")
    reg = json.load(open(reg_p)); cells = O.cell_definitions(reg); O.assert_registry_exact(cells, seal["k_total"])
    for c in cells:
        O.assert_entry_offset_ok(c)
    x_p = os.path.join(FAMILY_DIR, "runs", seal["x_run"], "X_v2.parquet")
    if _dig(x_p) != seal["x_v2_parquet_sha256_16"]:
        raise HardStop("X_v2 digest mismatch")
    cur = json.load(open(os.path.join(CANON_DIR, "CURRENT.json")))
    if _dig(cur["derived_parquet"]) != seal["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical changed")
    ps, slip = O.sacred_pathsim()
    O.assert_no_local_pathsim(open(__file__).read(), "direct runner")
    if PARTITION_IDIOM.search(open(__file__).read().split('"""', 2)[-1]):
        raise HardStop("index-modulo partition idiom found in the direct runner")
    # ── authority + first-run classification (records) ──
    auth = recover_authority()
    json.dump(auth, open(os.path.join(FAMILY_DIR, "PATHSIM_SEMANTICS_AUTHORITY.json"), "w"), indent=1, default=str)
    json.dump(dict(run_id=FIRST_RUN, status="EXPOSED_EXECUTION_WITH_PATHSIM_SELECTION_SEMANTICS_DEVIATION",
                   deviation="row outcomes were produced by five disjoint mod-5 masks so the engine's 5-bar cooldown never suppressed a row; "
                             "the sealed pre-outcome authority registers direct signal-mask semantics (cooldown included) — see PATHSIM_SEMANTICS_AUTHORITY.json",
                   preserved=["row_outcomes_h*.parquet", "results_300.parquet", "results_300.json", "summary.json", "input_census.json", "COMPLETED",
                              "FIRST_OUTCOME_EXECUTION_V1.json", "first outcome access timestamp"],
                   results_status="PROVISIONAL (mod-5 estimand); the direct execution is the POST_EXPOSURE_EXECUTION_CORRECTION",
                   classified_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
              open(os.path.join(first, "EXECUTION_CLASSIFICATION.json"), "w"), indent=1)
    run = RunSpace(run_id=time.strftime("OUTDIRECT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led = O.ledger_record_access(run.run_id, O.SEAL_V2_1_SHA256)
    led["events"][-1]["note"] = "POST_EXPOSURE_EXECUTION_CORRECTION: direct sacred _pathsim per registered cell mask"
    json.dump(led, open(O.LEDGER, "w"), indent=1)
    log(f"{run.run_id}: authority verdict {auth['verdict']}; outcome access count -> {led['outcome_access_count']}")
    # ── inputs ──
    needed = sorted({"ticker", "session", "base", "breakout", "gap_bucket", "price_bucket", "dv_bucket", "mkt_open1h", "open1h_rvol", "base_state"}
                    | {col for c in cells for col in c["columns"]} | {c["status_column"] for c in cells if c["kind"] == "state"})
    X = pd.read_parquet(x_p, columns=needed); X["session"] = X["session"].astype(str)
    frames = load_frames(cur["derived_parquet"], X["ticker"].unique())
    # ── class pools: every cell run directly; state FALSE sides as control-only masks ──
    classes = {}
    for c in cells:
        classes.setdefault((c["family"], tuple(c["columns"])), []).append(c)
    trades = {}; cons = {}; t0 = time.time(); k = 0
    for key, cs in classes.items():
        for c in cs:
            ins, ev, tr_mask = O.cell_state(X, c)
            keys = X.loc[tr_mask, ["ticker", "session"]]
            tr, cn = direct_trades(ps, frames, keys)
            tr["cell_id"] = c["claim_id"]; trades[c["claim_id"]] = tr; cons[c["claim_id"]] = cn
            k += 1
            if k % 25 == 0:
                log(f"  direct _pathsim: {k}/{len(cells)} cells ({time.time() - t0:.0f}s)")
        if cs[0]["kind"] == "state":
            ins, ev, tr_mask = O.cell_state(X, cs[0])
            keys = X.loc[ev & ~tr_mask, ["ticker", "session"]]
            tr, cn = direct_trades(ps, frames, keys)
            tr["cell_id"] = cs[0]["claim_id"] + "|FALSE_CONTROL_ONLY"; trades[tr["cell_id"].iloc[0] if len(tr) else cs[0]["claim_id"] + "|FALSE_CONTROL_ONLY"] = tr
            cons[cs[0]["claim_id"] + "|FALSE_CONTROL_ONLY"] = cn
    all_tr = pd.concat([t for t in trades.values() if len(t)], ignore_index=True)
    all_tr.to_parquet(os.path.join(run.dir, "direct_trades.parquet"), index=False)
    run.write_atomic("selection_conservation.json", cons)
    # A10 exposure on direct trades
    a10 = json.load(open(os.path.join(CANON_DIR, "dividend_reference", "MATERIALITY_CENSUS_A10.json")))
    ge5 = pd.read_parquet(a10["table"])[["ticker", "ex_date"]]; ge5["ex_date"] = ge5["ex_date"].astype(str)
    m = all_tr[["ticker", "session", "date_in", "date_out"]].merge(ge5, on="ticker", how="left")
    m["exp"] = m["ex_date"].notna() & (m["date_in"] < m["ex_date"]) & (m["ex_date"] <= m["date_out"])
    exp = m.groupby(["ticker", "session"])["exp"].any().rename("dist_exposed").reset_index()
    xs = X[["ticker", "session", "price_bucket", "dv_bucket", "gap_bucket", "base_state"]]
    # ── statistics per cell on the direct population ──
    mod5 = {r["claim_id"]: r for r in json.load(open(os.path.join(first, "results_300.json")))}
    results, day_series = [], {}
    for key, cs in classes.items():
        parts = [trades[c["claim_id"]] for c in cs] + ([trades[cs[0]["claim_id"] + "|FALSE_CONTROL_ONLY"]] if cs[0]["kind"] == "state" else [])
        pool = pd.concat([p for p in parts if len(p)], ignore_index=True)
        pool = pool[pool["ret"].notna()].reset_index(drop=True)
        ctrl, noth = O.loo_control(pool)
        pool["control"] = ctrl; pool["n_others"] = noth
        pool["edge"] = np.where(pool["n_others"] >= O.MIN_CONTROL, pool["ret"] - pool["control"], np.nan)
        pool = pool.merge(exp, on=["ticker", "session"], how="left").merge(xs, on=["ticker", "session"], how="left")
        for c in cs:
            obs = pool[pool["cell_id"] == c["claim_id"]]
            e = obs.dropna(subset=["edge"])
            ds = O.day_stats(e); ts = O.trade_stats(obs) if len(obs) else dict(n_obs=0)
            cn = cons[c["claim_id"]]
            rec = dict(claim_id=c["claim_id"], kind=c["kind"], family=c["family"], feature=c["feature"], bin=c["bin"], logic=c["logic"], resolution=c["resolution"],
                       sample_authority=c["sample_authority"], as_of=c["as_of"], entry=c["entry"], entry_offset_from_D=c["entry_offset_from_D"],
                       **cn, pathsim_defined_n=int(len(obs)), unmatched_n=int(obs["edge"].isna().sum()) if len(obs) else 0,
                       median_edge=ds["median_edge"], n_days=ds["n_days"], day_win=ds["day_win"], top2_share=ds["top2_share"],
                       per_year=ds["per_year"], days_per_year=ds["days_per_year"], **{k2: v for k2, v in ts.items()})
            yrs = {y: v for y, v in ds["per_year"].items() if ds["days_per_year"].get(y, 0) >= O.GATES["build"]["min_days_per_year"]}
            rec.update(positive_years=sum(1 for v in yrs.values() if v > 0), years_counted=len(yrs),
                       worst_year=(min(yrs.values()) if yrs else None), best_year=(max(yrs.values()) if yrs else None))
            dist = {}
            for lab, g in (("all", e), ("exposed", e[e["dist_exposed"] == True]), ("non_exposed", e[e["dist_exposed"] != True])):  # noqa: E712
                d = g.groupby("date_in")["edge"].median() if len(g) else pd.Series(dtype=float)
                dist[lab] = dict(n_obs=int(len(g)), n_days=int(len(d)), median_edge=(round(float(d.median()), 3) if len(d) else None))
            rec["distribution_diagnostic"] = dist
            strata = {}
            for s in ("price_bucket", "dv_bucket", "gap_bucket", "base_state"):
                strata[s] = {str(val): dict(n_days=int(len(d)), median_edge=round(float(d.median()), 3))
                             for val, g in e.groupby(s, dropna=True) for d in [g.groupby("date_in")["edge"].median()]}
            rec["strata"] = strata
            day_series[c["claim_id"]] = ds.get("day_series", pd.Series(dtype=float))
            results.append(rec)
    from overfit_stats import dsr, sharpe
    trial_srs = [sharpe(day_series[r["claim_id"]].to_numpy()) if len(day_series[r["claim_id"]]) >= 2 else 0.0 for r in results]
    for rec in results:
        s = day_series[rec["claim_id"]].to_numpy(float)
        if len(s) >= 3:
            dp = dsr(s, trial_srs, n_trials=seal["k_total"]); dn = dsr(-s, [-x for x in trial_srs], n_trials=seal["k_total"])
            rec.update(dsr=dp["dsr"], sr=dp["sr"], sr_star=dp["sr_star"], dsr_neg=dn["dsr"])
        else:
            rec.update(dsr=None, sr=None, sr_star=None, dsr_neg=None)
        rec["thin"] = rec["n_days"] < O.THIN_DAYS or rec["n_obs"] < O.THIN_OBS
        rec["classification"] = O.classify(dict(n_days=rec["n_days"], n_obs=rec["n_obs"], median_edge=rec["median_edge"] or 0.0, day_win=rec["day_win"] or 0.0,
                                                per_year=rec["per_year"], days_per_year=rec["days_per_year"]), rec["dsr"] or 0.0, rec["dsr_neg"] or 0.0)
        m5 = mod5[rec["claim_id"]]
        rec["vs_mod5"] = dict(n_obs=(m5["n_obs"], rec["n_obs"]), median_edge=(m5["median_edge"], rec["median_edge"]),
                              median_edge_diff=(None if m5["median_edge"] is None or rec["median_edge"] is None else round(rec["median_edge"] - m5["median_edge"], 3)),
                              day_win=(m5["day_win"], rec["day_win"]), positive_years=(m5["positive_years"], rec["positive_years"]),
                              worst_year=(m5["worst_year"], rec["worst_year"]), dsr=(m5["dsr"], rec["dsr"]),
                              classification=(m5["classification"], rec["classification"]), changed=m5["classification"] != rec["classification"])
    order = {c["claim_id"]: i for i, c in enumerate(cells)}
    results.sort(key=lambda r: order[r["claim_id"]])
    O.assert_result_reconciles([r["claim_id"] for r in results], cells)
    res_df = pd.DataFrame([{k2: (json.dumps(v, default=str) if isinstance(v, (dict, list, tuple)) else v) for k2, v in r.items()} for r in results])
    res_p = os.path.join(run.dir, "results_300_direct.parquet"); res_df.to_parquet(res_p, index=False)
    run.write_atomic("results_300_direct.json", results)
    cls = {}
    for r in results:
        cls[r["classification"]] = cls.get(r["classification"], 0) + 1
    changed = [dict(claim_id=r["claim_id"], mod5=r["vs_mod5"]["classification"][0], direct=r["vs_mod5"]["classification"][1]) for r in results if r["vs_mod5"]["changed"]]
    diffs = np.array([r["vs_mod5"]["median_edge_diff"] for r in results if r["vs_mod5"]["median_edge_diff"] is not None], float)
    supp = np.array([r["cooldown_suppressed_n"] / r["signal_true_n"] if r["signal_true_n"] else np.nan for r in results], float)
    summary = dict(run_id=run.run_id, label="POST_EXPOSURE_EXECUTION_CORRECTION", first_run=FIRST_RUN, authority_verdict=auth["verdict"],
                   seal_v2_1_sha256=O.SEAL_V2_1_SHA256, registry_sha256_16=seal["registry_sha256_16"], k=seal["k_total"], x_sha256_16=seal["x_v2_parquet_sha256_16"],
                   pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, params=O.PARAMS, gates=O.GATES, min_control=O.MIN_CONTROL,
                   outcome_access_count=led["outcome_access_count"], pathsim_calls=len(trades),
                   reconciliation=dict(tested=len(results), missing=0, extra=0, duplicate=0),
                   classification_census_direct=cls, classification_changes=changed,
                   median_edge_diff_direct_minus_mod5=dict(median=round(float(np.median(diffs)), 3), p05=round(float(np.percentile(diffs, 5)), 3), p95=round(float(np.percentile(diffs, 95)), 3), n=int(len(diffs))),
                   cooldown_suppression_share=dict(median=round(float(np.nanmedian(supp)), 3), max=round(float(np.nanmax(supp)), 3)),
                   completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summary)
    run.complete("results_300_direct.parquet")
    run.write_atomic("STATUS.json", dict(run_id=run.run_id, status="POST_EXPOSURE_EXECUTION_CORRECTION_COMPLETED", seal_v2_1_sha256=O.SEAL_V2_1_SHA256,
                                         results_sha256_16=_dig(res_p)))
    gov = dict(record_id="OPENING_VOLUME_DYNAMICS_FIRST_OUTCOME_EXECUTION_V1_CORRECTION_DIRECT_PATHSIM", run_id=run.run_id, first_run=FIRST_RUN,
               label="POST_EXPOSURE_EXECUTION_CORRECTION — not independent confirmation", authority=auth["verdict"],
               authority_record_sha256_16=_dig(os.path.join(FAMILY_DIR, "PATHSIM_SEMANTICS_AUTHORITY.json")),
               first_run_classification_sha256_16=_dig(os.path.join(first, "EXECUTION_CLASSIFICATION.json")),
               parent_seal_v2_1_sha256=O.SEAL_V2_1_SHA256, registry_sha256_16=seal["registry_sha256_16"], k=seal["k_total"],
               pathsim=dict(src_sha256_16=O.PATHSIM_SRC_SHA, params=O.PARAMS, calls=len(trades), mode="ONE direct call per registered cell mask (+ one FALSE-side control-only mask per state cell)"),
               results_sha256_16=_dig(res_p), summary_sha256_16=_dig(os.path.join(run.dir, "summary.json")),
               selection_conservation_sha256_16=_dig(os.path.join(run.dir, "selection_conservation.json")),
               outcome_access_count=led["outcome_access_count"], runner_sha256_16=_dig(__file__),
               written_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    gp = os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION_V1_CORRECTION_DIRECT_PATHSIM.json")
    if os.path.exists(gp):
        raise HardStop("correction record already exists")
    json.dump(gov, open(gp, "w"), indent=1, default=str)
    log(f"COMPLETED {run.run_id}: {cls}; classification changes {len(changed)}")
    return summary


if __name__ == "__main__":
    if "--authority" in sys.argv:
        a = recover_authority()
        print(json.dumps({k: v for k, v in a.items() if k != "all_hits"}, indent=1, default=str))
        print("hits:", len(a["all_hits"]))
        for h in a["all_hits"][:40]:
            print(f"  [{h['artifact']} {h['sha256_16']}] /{h['pattern']}/: …{h['quote'][:220]}…")
    else:
        print(json.dumps(run(), indent=1, default=str))
