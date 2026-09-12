"""OPENING_VOLUME_DYNAMICS — OUTCOME PHASE · FIRST HISTORICAL EXECUTION (V1 of the outcome runner).

Execution authority: SEAL_V2_1 (ovd_seal_v2.execution_authority — presenting V1 / V2 is rejected). k = 300.
The sacred exit engine is backend/edge_replay.py::_pathsim, imported and called UNMODIFIED (source digest checked
before every call). Frozen primary configuration: mode=trail, atr_k=12 (trail = clip(12·ATR%, 15%, 60%)),
maxh=60, slip=SLIP=0.0015. Entry is claim-specific by construction: every X row enters at the open of the
session AFTER the row (D+1 for rows anchored at D; D+2 for the Logic-4 rows anchored at D+1).

ROW-OUTCOME TABLE (execution-design decision recorded BEFORE any outcome is computed): the registered same-day
control needs every observation's own _pathsim outcome. _pathsim carries a 5-bar same-ticker cooldown
(`i - last < 5` → skip), which would silently thin any pooled mask (the artifact catalogued in this family's
history). Therefore every eligible row is evaluated exactly once by calling the UNMODIFIED engine over five
disjoint masks (bar index mod 5): consecutive taken signals in one mask are ≥ 5 bars apart, so the cooldown
never suppresses a row. The engine, its fills, trail, slip and horizon are untouched. For reconciliation the
cooldown-thinned trade count of a direct cell-mask call is reported for the seven V2.1 cells (descriptive).

Primary statistic (frozen, V1 A7): control_d(i) = median of the OTHER family rows on the same ENTRY date on
which the cell's feature(s) are evaluable (self excluded, ≥ 20 others else UNAVAILABLE); edge_i = ret_i −
control; day_edge_d = median(edge_i); cell = median over days; day_win; n_days. Multiplicity = overfit_stats.dsr
with the day-edge series as periods and the 300 cells' day-edge Sharpes as the trial family, n_trials = 300.
Horizons 3/5/10/20 are DESCRIPTIVE only. The A10 distribution diagnostic is computed after actual exits exist.

Classification uses the registered analysis standard, written here BEFORE any outcome exists:
  BUILD_CANDIDATE : median day-edge > 0 · day_win > 50% · ≥ 4 positive years (years with ≥ 5 days) ·
                    worst year ≥ −2.0 pp · DSR ≥ 0.95 (k = 300) · not THIN
  VETO_CANDIDATE  : mirror (median < 0 · day_win < 50% · ≥ 4 negative years · best year ≤ +2.0 · DSR of the
                    negated series ≥ 0.95 · not THIN)
  DESCRIPTIVE_ONLY: THIN = n_days < 30 or n_obs < 100 (stays in k)          NULL: everything else
BUILD_CANDIDATE ≠ confirmed edge; VETO_CANDIDATE ≠ production veto. After this run the family is search-exposed.
"""
from __future__ import annotations
import os, sys, re, json, time, hashlib, inspect, glob                  # noqa: E402
import numpy as np                                                      # noqa: E402
import pandas as pd                                                     # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import HardStop, RunSpace                                # noqa: E402
import ovd_seal_v2 as S2                                                # noqa: E402
import ovd_registry_v2 as R2                                            # noqa: E402
import ovd_diagnostics as DG                                            # noqa: E402

FAMILY_DIR = S2.FAMILY_DIR
CANON_DIR = S2.CANON_DIR
LEDGER = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
SEAL_V2_1_SHA256 = "4faaa234fe169f3f1b24aeb8f50ea7e7c2823adea9595115518c55a448bb230f"
PATHSIM_SRC_SHA = "0e74668f554910de"
PARAMS = dict(mode="trail", atr_k=12.0, trail_fallback=0.25, stop=0.10, target=0.25, maxh=60, slip=0.0015,
              trail_rule="clip(12 * atr_14 / close at the signal bar, 0.15, 0.60)")
HORIZONS = [3, 5, 10, 20, 60]            # 60 = primary; others DESCRIPTIVE / SENSITIVITY ONLY
MIN_CONTROL = 20
THIN_DAYS, THIN_OBS = 30, 100
GATES = dict(build=dict(median_gt=0.0, day_win_gt=50.0, pos_years_ge=4, worst_year_ge=-2.0, dsr_ge=0.95, min_days_per_year=5),
             veto=dict(median_lt=0.0, day_win_lt=50.0, neg_years_ge=4, best_year_le=2.0, dsr_ge=0.95, min_days_per_year=5),
             thin=dict(n_days_lt=THIN_DAYS, n_obs_lt=THIN_OBS),
             source="registered analysis standard (feedback-analysis-standard L1: years ≥4/6, worst ≥ −2pp; L3: DSR at global k); "
                    "thresholds recorded before any outcome exists")
K_EXPECTED = 300
LOGIC_OF = {  # three-resolution tags for reporting (never a ranking)
    "OPEN1H_RVOL_LEVEL": (1, "60m"), "OPEN1H_RAMP_3D": (1, "60m"), "OPEN1H_RAMP_MAG": (1, "60m"), "OPEN1H_RAMP_5D": (1, "60m"),
    "BASE_DRYUP": (1, "60m"), "DRYUP_TO_EXPANSION": (1, "60m"), "IDIO_OPEN1H": (1, "60m"),
    "OPEN15_BREADTH": (1, "15m"), "OPEN15_CONCENTRATION": (1, "15m"), "OPEN15_PERSISTENCE": (1, "15m"), "OPEN15_SHAPE": (1, "15m"),
    "VOLUME_TRANSFER": (1, "15m"), "EFFORT_VS_RESULT": (1, "60m"), "INTRADAY_BREADTH_SHIFT": (1, "60m"),
    "PRIOR_HV_VOLUME_RECLAIM": (2, "60m"), "PRIOR_HV_DAY_RESPONSE": (2, "60m"),
    "OPEN30_SUSTAINED_BUILD_3D": (1, "30m"), "FULL_OPENING_RECLAIM_30": (2, "30m"),
    "CLOSE60_DOMINANCE": (3, "60m"), "CLOSE30_DOMINANCE": (3, "30m"),
    "CLOSE60_TO_NEXT_OPEN60_HANDOFF": (4, "60m"), "CLOSE30_TO_NEXT_OPEN30_HANDOFF": (4, "30m")}


def _dig(p: str, n: int = 16) -> str:
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def _jdig(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]


# ── engine identity guards ─────────────────────────────────────────────────────────────────
def sacred_pathsim():
    """The ONLY exit engine. Digest-checked on every fetch."""
    import edge_replay as E
    sha = hashlib.sha256(inspect.getsource(E._pathsim).encode()).hexdigest()[:16]
    if sha != PATHSIM_SRC_SHA:
        raise HardStop(f"_pathsim source digest {sha} != bound {PATHSIM_SRC_SHA}")
    return E._pathsim, E.SLIP


def assert_no_local_pathsim(source: str, where: str = "runner") -> None:
    """A local exit engine is forbidden: no function named like a path simulator, no trail/stop
    arithmetic assignments (reading the engine's OUTPUT fields is fine)."""
    if re.search(r"^\s*def\s+(_?pathsim\w*|\w*path_sim\w*|\w*simulate\w*|\w*trail_exit\w*|\w*exit_engine\w*)\s*\(", source, re.M | re.I):
        raise HardStop(f"{where}: local path-simulation function defined — only edge_replay._pathsim may be used")
    if re.search(r"^\s*(trail_level|stop_level|trail_price|peak)\s*=\s*", source, re.M) or re.search(r"\bpk\s*\*\s*\(1\s*-", source):
        raise HardStop(f"{where}: exit-engine arithmetic found — local reimplementation forbidden")


# ── ledger ─────────────────────────────────────────────────────────────────────────────────
def ledger_read() -> dict:
    if not os.path.exists(LEDGER):
        return dict(outcome_access_count=0, events=[])
    return json.load(open(LEDGER))


def ledger_record_access(run_id: str, seal_sha: str) -> dict:
    """Increments outcome_access_count exactly once per authorised evaluation start; never resets."""
    led = ledger_read()
    if any(e["run_id"] == run_id for e in led["events"]):
        raise HardStop(f"ledger already records an access for {run_id}")
    prev = int(led["outcome_access_count"])
    led["outcome_access_count"] = prev + 1
    led["events"].append(dict(run_id=run_id, count_before=prev, count_after=prev + 1, seal_v2_1_sha256=seal_sha,
                              at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), note="first authorised _pathsim evaluation follows immediately"))
    tmp = LEDGER + ".tmp"
    json.dump(led, open(tmp, "w"), indent=1); os.replace(tmp, LEDGER)
    return led


# ── registry / cell masks ──────────────────────────────────────────────────────────────────
def parse_bins(bins: list[str]):
    """Numeric bin strings -> list of (lo, lo_incl, hi, hi_incl). Right-open by default; a bin
    following a closed upper edge ('<=0') is left-open so the bins partition the line."""
    out, prev_hi_incl = [], False
    for b in bins:
        b = b.strip()
        m = re.fullmatch(r"\((-?[\d.]+),\s*(-?[\d.]+)\)", b)
        if m:
            out.append((float(m.group(1)), False, float(m.group(2)), False)); prev_hi_incl = False; continue
        if b.startswith("<="):
            out.append((-np.inf, False, float(b[2:]), True)); prev_hi_incl = True; continue
        if b.startswith("<"):
            out.append((-np.inf, False, float(b[1:]), False)); prev_hi_incl = False; continue
        if b.startswith(">="):
            out.append((float(b[2:]), True, np.inf, False)); prev_hi_incl = False; continue
        if b.startswith(">"):
            out.append((float(b[1:]), not prev_hi_incl, np.inf, False)); prev_hi_incl = False; continue
        m = re.fullmatch(r"(-?[\d.]+)-(-?[\d.]+)", b)
        if m:
            out.append((float(m.group(1)), not prev_hi_incl, float(m.group(2)), False)); prev_hi_incl = False; continue
        return None            # categorical
    return out


def bin_mask(series: pd.Series, bins: list[str], b: str) -> np.ndarray:
    iv = parse_bins(bins)
    v = series.to_numpy()
    if iv is None:
        if pd.api.types.is_numeric_dtype(series):
            return (v == float(b)) & ~pd.isna(v)
        return series.astype(object).to_numpy() == b
    lo, lo_incl, hi, hi_incl = iv[bins.index(b)]
    x = series.to_numpy(float)
    ok = ~np.isnan(x)
    left = (x >= lo) if lo_incl else (x > lo)
    right = (x <= hi) if hi_incl else (x < hi)
    return ok & left & right


def cell_definitions(reg: dict) -> list[dict]:
    """Every registered cell -> (claim_id, family, feature(s), bin, columns, sample_sql, as_of, entry)."""
    v1 = reg["v1_registry"]; feats = v1["features"]
    fam_sql = R2.SAMPLE_SQL
    out = []
    for c in reg["cells"]:
        if c["kind"] == "single":
            col, bins, _ = feats[c["feature"]]
            out.append(dict(claim_id=f"{c['family']}|{c['feature']}|{c['bin']}", kind="single", family=c["family"], feature=c["feature"],
                            bin=c["bin"], columns=[col], bins=[bins], sample_sql=fam_sql[c["family"]], as_of="10:30 NY D (V1 registry)",
                            entry="D+1 open", entry_offset_from_D=1, logic=LOGIC_OF.get(c["feature"], (0, "1D"))[0],
                            resolution=LOGIC_OF.get(c["feature"], (0, "1D"))[1], sample_authority=f"V1 family {c['family']}: {v1['samples'][c['family']]}"))
        elif c["kind"] == "interaction":
            f1, f2 = c["feature"].split("x", 1)
            b1, b2 = c["bin"].split("|", 1)
            col1, bins1 = ("gap_bucket", list(v1["strata"]["gap_bucket"])) if f1 == "GAP_BUCKET" else (feats[f1][0], feats[f1][1])
            col2, bins2 = feats[f2][0], feats[f2][1]
            out.append(dict(claim_id=f"{c['family']}|{c['feature']}|{c['bin']}", kind="interaction", family=c["family"], feature=c["feature"],
                            bin=c["bin"], columns=[col1, col2], bins=[bins1, bins2], sub_bins=[b1, b2], sample_sql=fam_sql[c["family"]],
                            as_of="10:30 NY D (V1 registry)", entry="D+1 open", entry_offset_from_D=1, logic=1, resolution="mixed",
                            sample_authority=f"V1 family {c['family']}: {v1['samples'][c['family']]}"))
        elif c["kind"] == "state":
            out.append(dict(claim_id=f"{c['family']}|{c['feature']}|{c['bin']}", kind="state", family=c["family"], feature=c["feature"],
                            bin=c["bin"], columns=[c["column"]], bins=None, status_column=c["status_column"], sample_sql=c["sample_sql"],
                            as_of=c["as_of"], entry=c["entry"], entry_offset_from_D=R2.ENTRY_OFFSET[c["feature"]],
                            logic=LOGIC_OF[c["feature"]][0], resolution=LOGIC_OF[c["feature"]][1], sample_authority=c["sample_authority"]))
        else:
            raise HardStop(f"unknown cell kind {c}")
    return out


def assert_registry_exact(cells: list[dict], k_expected: int = K_EXPECTED) -> None:
    ids = [c["claim_id"] for c in cells]
    if len(ids) != k_expected:
        raise HardStop(f"registry entries {len(ids)} != k {k_expected}")
    if len(set(ids)) != len(ids):
        raise HardStop("duplicate claim identities")


def assert_result_reconciles(result_ids: list[str], cells: list[dict]) -> None:
    want = {c["claim_id"] for c in cells}; got = list(result_ids)
    if len(got) != len(set(got)):
        raise HardStop("duplicate cells in the result table")
    if set(got) - want:
        raise HardStop(f"extra cells in the result table: {sorted(set(got) - want)[:3]}")
    if want - set(got):
        raise HardStop(f"missing registered cells: {sorted(want - set(got))[:3]}")


def assert_entry_offset_ok(cell: dict) -> None:
    need = R2.ENTRY_OFFSET.get(cell["feature"], 1)
    if cell["entry_offset_from_D"] != need:
        raise HardStop(f"wrong entry offset for {cell['claim_id']}: {cell['entry_offset_from_D']} != {need}")


def sample_mask(X: pd.DataFrame, sample_sql: str) -> np.ndarray:
    base = X["base"].fillna(False).to_numpy(bool); brk = X["breakout"].fillna(False).to_numpy(bool)
    return {"base AND NOT breakout": base & ~brk, "breakout": brk, "TRUE": np.ones(len(X), bool)}[sample_sql]


def cell_state(X: pd.DataFrame, cell: dict):
    """Returns (in_sample, evaluable, true_state) boolean arrays over X rows."""
    ins = sample_mask(X, cell["sample_sql"])
    if cell["kind"] == "single":
        s = X[cell["columns"][0]]
        ev = ~pd.isna(s).to_numpy()
        tr = bin_mask(s, cell["bins"][0], cell["bin"])
    elif cell["kind"] == "interaction":
        s1, s2 = X[cell["columns"][0]], X[cell["columns"][1]]
        ev = ~pd.isna(s1).to_numpy() & ~pd.isna(s2).to_numpy()
        tr = bin_mask(s1, cell["bins"][0], cell["sub_bins"][0]) & bin_mask(s2, cell["bins"][1], cell["sub_bins"][1])
    else:
        st = X[cell["status_column"]].astype(object).to_numpy()
        ev = (st == "TRUE") | (st == "FALSE")
        tr = st == "TRUE"
        if ((st == "NOT_ELIGIBLE") & ins).any() or ((st != "NOT_ELIGIBLE") & ~ins).any():
            raise HardStop(f"status column / sample mask disagree for {cell['claim_id']}")
    return ins, ev & ins, tr & ins


# ── input census (frozen BEFORE outcomes) ──────────────────────────────────────────────────
def input_census(X: pd.DataFrame, cells: list[dict]) -> dict:
    rows = {}
    for c in cells:
        ins, ev, tr = cell_state(X, c)
        rows[c["claim_id"]] = dict(feature=c["feature"], bin=c["bin"], family=c["family"], sample_authority=c["sample_authority"],
                                   as_of=c["as_of"], entry=c["entry"], eligible_n=int(ins.sum()), TRUE=int(tr.sum()),
                                   FALSE=int((ev & ~tr).sum()), UNAVAILABLE=int((ins & ~ev).sum()))
    # partition conservation for every single feature within its family: bins cover all evaluable rows once
    for fam in ("A", "B"):
        for feat in {c["feature"] for c in cells if c["kind"] == "single" and c["family"] == fam}:
            tot_true = sum(rows[c["claim_id"]]["TRUE"] for c in cells if c["kind"] == "single" and c["family"] == fam and c["feature"] == feat)
            any_cell = next(c for c in cells if c["kind"] == "single" and c["family"] == fam and c["feature"] == feat)
            ins, ev, _ = cell_state(X, any_cell)
            if tot_true != int(ev.sum()):
                raise HardStop(f"bins of {feat} do not partition family {fam}: {tot_true} != {int(ev.sum())}")
    return rows


def reconcile_v2_1_census(census: dict, cells: list[dict], build_report: dict) -> dict:
    """The seven V2.1 cells must reproduce the pre-outcome census sealed with SEAL_V2_1 exactly."""
    out = {}
    for c in cells:
        if c["kind"] != "state":
            continue
        sealed = build_report["claims"][c["feature"]]["cells"][c["family"]]
        now = census[c["claim_id"]]
        same = (sealed["TRUE"], sealed["FALSE"], sealed["UNAVAILABLE"], sealed["n_sample"]) == (now["TRUE"], now["FALSE"], now["UNAVAILABLE"], now["eligible_n"])
        out[c["claim_id"]] = dict(sealed=(sealed["TRUE"], sealed["FALSE"], sealed["UNAVAILABLE"]), now=(now["TRUE"], now["FALSE"], now["UNAVAILABLE"]), match=same)
        if not same:
            raise HardStop(f"pre-outcome census changed for {c['claim_id']}: sealed {out[c['claim_id']]['sealed']} vs now {out[c['claim_id']]['now']}")
    return out


# ── row outcomes via the sacred engine ─────────────────────────────────────────────────────
def row_outcomes(canon_parquet: str, xkeys: pd.DataFrame, maxh: int, log=print) -> pd.DataFrame:
    """Every eligible X row's own path outcome from the open of the NEXT session, via _pathsim UNMODIFIED
    over five disjoint masks (bar index mod 5) — see the module docstring."""
    ps, slip = sacred_pathsim()
    import duckdb
    con = duckdb.connect()
    con.register("xk", xkeys)
    canon = con.execute(f"SELECT c.ticker, c.session_date AS date, c.open, c.high, c.low, c.close, c.atr_14 "
                        f"FROM read_parquet('{canon_parquet}') c WHERE c.ticker IN (SELECT DISTINCT ticker FROM xk) ORDER BY 1, 2").df()
    con.close()
    canon["date"] = canon["date"].astype(str)
    xset = set(zip(xkeys["ticker"].to_numpy(), xkeys["session"].astype(str).to_numpy()))
    frames = []
    t0 = time.time(); n_tk = canon["ticker"].nunique()
    for k, (tk, g) in enumerate(canon.groupby("ticker", sort=False)):
        g = g.reset_index(drop=True)
        elig = np.fromiter(((tk, d) in xset for d in g["date"].to_numpy()), bool, len(g))
        pos = np.arange(len(g))
        for r in range(5):
            g[f"M{r}"] = elig & (pos % 5 == r)
        nxt = dict(zip(g["date"].to_numpy()[1:], g["date"].to_numpy()[:-1]))     # date_in -> signal session
        last_date = g["date"].iloc[-1]; n = len(g)
        for r in range(5):
            if not g[f"M{r}"].any():
                continue
            tr = ps({tk: g}, f"M{r}", PARAMS["mode"], PARAMS["stop"], PARAMS["target"], PARAMS["trail_fallback"], maxh,
                    slip=None, atr_k=PARAMS["atr_k"])
            if len(tr):
                tr["session"] = tr["date_in"].map(nxt)
                sig_pos = tr["session"].map(dict(zip(g["date"], pos)))
                tr["censored"] = ((sig_pos + 1 + maxh) > n) & (tr["date_out"] == last_date)
                frames.append(tr)
        if (k + 1) % 500 == 0:
            log(f"  _pathsim maxh={maxh}: {k + 1:,}/{n_tk:,} tickers ({time.time() - t0:.0f}s)")
    out = pd.concat(frames, ignore_index=True)
    out["ret"] = out["ret"].astype(float) * 100.0
    if out.duplicated(["ticker", "session"]).any():
        raise HardStop("a row received two outcomes")
    return out[["ticker", "session", "date_in", "date_out", "ret", "mae", "mfe", "hold", "risk", "censored"]]


# ── primary statistic ──────────────────────────────────────────────────────────────────────
def loo_control(df: pd.DataFrame, value_col: str = "ret") -> tuple[np.ndarray, np.ndarray]:
    """Leave-one-out median of `value_col` within each date_in group + the count of OTHER members.
    df must carry date_in and value_col; returns arrays aligned with df.index order."""
    v = df[value_col].to_numpy(float)
    ctrl = np.full(len(df), np.nan); n_oth = np.zeros(len(df), int)
    for _, idx in df.groupby("date_in", sort=False).indices.items():
        vals = v[idx]; m = len(vals)
        n_oth[idx] = m - 1
        if m < 2:
            continue
        order = np.argsort(vals, kind="mergesort"); sv = vals[order]; rank = np.empty(m, int); rank[order] = np.arange(m)
        rem = m - 1
        if rem % 2 == 1:
            mid = rem // 2
            med = np.where(rank <= mid, sv[np.minimum(mid + 1, m - 1)], sv[mid])
        else:
            a, b = rem // 2 - 1, rem // 2
            va = np.where(rank <= a, sv[np.minimum(a + 1, m - 1)], sv[a])
            vb = np.where(rank <= b, sv[np.minimum(b + 1, m - 1)], sv[b])
            med = (va + vb) / 2.0
        ctrl[idx] = med
    return ctrl, n_oth


def day_stats(edge: pd.DataFrame) -> dict:
    """edge: rows of one cell with columns date_in, edge (defined). Frozen aggregation order."""
    if not len(edge):
        return dict(n_days=0, median_edge=None, day_win=None, top2_share=None, per_year={}, days_per_year={})
    d = edge.groupby("date_in")["edge"].median()
    e = d.to_numpy(); years = pd.to_datetime(d.index).year
    pos = e[e > 0].sum(); top2 = np.sort(e)[-2:].sum() if len(e) >= 2 else e.sum()
    per_year = {int(y): round(float(np.median(e[years == y])), 3) for y in sorted(set(years))}
    days_per_year = {int(y): int((years == y).sum()) for y in sorted(set(years))}
    return dict(n_days=int(len(e)), median_edge=round(float(np.median(e)), 3), day_win=round(float((e > 0).mean() * 100), 1),
                top2_share=round(float(top2 / pos * 100), 1) if pos > 0 else None, per_year=per_year, days_per_year=days_per_year,
                day_series=d)


def trade_stats(tr: pd.DataFrame) -> dict:
    if not len(tr):
        return dict(n_obs=0)
    r = tr["ret"].to_numpy(float); wins = r > 0
    pf_d = -r[~wins].sum()
    by_tk = tr.groupby("ticker")["ret"].sum().sort_values(ascending=False); tot = by_tk[by_tk > 0].sum()
    top10 = by_tk.head(max(1, len(by_tk) // 10)).clip(lower=0).sum()
    return dict(n_obs=int(len(r)), raw_median=round(float(np.median(r)), 3), raw_mean=round(float(r.mean()), 3),
                win_rate=round(float(wins.mean() * 100), 1), profit_factor=round(float(r[wins].sum() / pf_d), 3) if pf_d > 0 else None,
                med_mae=round(float(tr["mae"].median() * 100), 2), med_mfe=round(float(tr["mfe"].median() * 100), 2),
                avg_hold=round(float(tr["hold"].mean()), 1), censored_n=int(tr["censored"].sum()),
                conc_top10pct_tickers=round(float(top10 / tot * 100), 1) if tot > 0 else None)


def classify(stat: dict, dsr_pos: float, dsr_neg: float) -> str:
    g = GATES
    if stat["n_days"] < g["thin"]["n_days_lt"] or stat["n_obs"] < g["thin"]["n_obs_lt"]:
        return "DESCRIPTIVE_ONLY"
    yrs = {y: v for y, v in stat["per_year"].items() if stat["days_per_year"].get(y, 0) >= g["build"]["min_days_per_year"]}
    if not yrs:
        return "NULL"
    pos_years = sum(1 for v in yrs.values() if v > 0); neg_years = sum(1 for v in yrs.values() if v < 0)
    worst, best = min(yrs.values()), max(yrs.values())
    b = g["build"]
    if stat["median_edge"] > b["median_gt"] and stat["day_win"] > b["day_win_gt"] and pos_years >= b["pos_years_ge"] \
            and worst >= b["worst_year_ge"] and dsr_pos >= b["dsr_ge"]:
        return "BUILD_CANDIDATE"
    v = g["veto"]
    if stat["median_edge"] < v["median_lt"] and stat["day_win"] < v["day_win_lt"] and neg_years >= v["neg_years_ge"] \
            and best <= v["best_year_le"] and dsr_neg >= v["dsr_ge"]:
        return "VETO_CANDIDATE"
    return "NULL"


# ── the run ────────────────────────────────────────────────────────────────────────────────
def preflight(log=print) -> dict:
    seal = S2.execution_authority()                          # V2.1 only; harness code drift → HardStop
    p = os.path.join(FAMILY_DIR, "SEAL_V2_1.json")
    if hashlib.sha256(open(p, "rb").read()).hexdigest() != SEAL_V2_1_SHA256 or seal["amendment"] != "OPENING_VOLUME_DYNAMICS_PRE_OUTCOME_RESEAL_V2_1":
        raise HardStop("SEAL_V2_1 digest / identity mismatch")
    S2.assert_v1_immutable()
    import ovd_seal_v2_1 as S21
    S21.assert_v2_immutable()
    reg_p = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V2_1.json")
    if _dig(reg_p) != seal["registry_sha256_16"]:
        raise HardStop("SEARCH_REGISTRY_V2_1.json digest != SEAL_V2_1")
    reg = json.load(open(reg_p))
    cells = cell_definitions(reg)
    assert_registry_exact(cells, seal["k_total"])
    if seal["k_total"] != K_EXPECTED:
        raise HardStop(f"k {seal['k_total']} != {K_EXPECTED}")
    for c in cells:
        assert_entry_offset_ok(c)
    cur = json.load(open(os.path.join(CANON_DIR, "CURRENT.json")))
    if cur["run_id"] != seal["canonical_1d"]["run_id"] or _dig(cur["derived_parquet"]) != seal["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical 1D authority changed")
    run_dir = os.path.join(FAMILY_DIR, "runs", seal["x_run"])
    x_p = os.path.join(run_dir, "X_v2.parquet")
    if _dig(x_p) != seal["x_v2_parquet_sha256_16"] or not RunSpace.is_current(run_dir, "X_v2.parquet"):
        raise HardStop("X_v2 digest / currency mismatch")
    if _dig(os.path.join(run_dir, "slots_v2.parquet")) != seal["source_15m"]["slots_extract_sha256_16"]:
        raise HardStop("15m slot extract changed")
    ps, slip = sacred_pathsim()
    if abs(slip - PARAMS["slip"]) > 1e-12:
        raise HardStop("SLIP changed")
    assert_no_local_pathsim(open(__file__).read())
    stale = [r for r in glob.glob(os.path.join(FAMILY_DIR, "runs", "OUT_*")) if RunSpace.is_current(r, "results_300.parquet")]
    led = ledger_read()
    if stale or led["outcome_access_count"] != 0:
        raise HardStop(f"a prior outcome run is CURRENT / ledger count {led['outcome_access_count']} — this is not the first execution")
    access = S2.assert_no_outcome_access()
    fx = _run_fixtures()
    return dict(seal_v2_1_sha256=SEAL_V2_1_SHA256, amendment=seal["amendment"], k=seal["k_total"], registry_sha256_16=seal["registry_sha256_16"],
                cells=cells, reg=reg, x_parquet=x_p, x_sha256_16=seal["x_v2_parquet_sha256_16"], x_run=seal["x_run"],
                canonical=seal["canonical_1d"], derived_parquet=cur["derived_parquet"], pathsim_src_sha256_16=PATHSIM_SRC_SHA,
                fixtures=fx, access=access, harness_code=seal["code"], v1_seal=S2.V1_SEAL_SHA256, v2_seal=S21.V2_SEAL_SHA256)


def _run_fixtures() -> dict:
    import subprocess, tempfile, xml.etree.ElementTree as ET
    files = list(S2.FIXTURE_FILES) + ["tests/test_ovd_v2_1.py", "tests/test_ovd_outcome.py"]
    paths = [os.path.join(HERE, f) for f in files]
    fd, xml_p = tempfile.mkstemp(prefix="ovd_out_fx_", suffix=".xml"); os.close(fd)
    try:
        r = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", f"--junitxml={xml_p}", *paths], cwd=HERE, capture_output=True, text=True)
        root = ET.parse(xml_p).getroot(); suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
        tests = sum(int(s.get("tests", 0)) for s in suites); bad = sum(int(s.get("failures", 0)) + int(s.get("errors", 0)) for s in suites)
    finally:
        try:
            os.remove(xml_p)
        except OSError:
            pass
    if r.returncode != 0 or bad or tests == 0:
        raise HardStop(f"fixtures not green: {tests} tests, {bad} failed\n{r.stdout[-3000:]}")
    return dict(passed=tests - bad, tests=tests, files={f: _dig(p) for f, p in zip(files, paths)})


def run(log=print) -> dict:
    pf = preflight(log)
    cells, reg = pf["cells"], pf["reg"]
    run = RunSpace(run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    log(f"run {run.run_id}: preflight OK, k={pf['k']}, fixtures {pf['fixtures']['passed']}/{pf['fixtures']['tests']}")
    needed = sorted({"ticker", "session", "base", "breakout", "gap_bucket", "price_bucket", "dv_bucket", "mkt_open1h", "open1h_rvol", "base_state"}
                    | {col for c in cells for col in c["columns"]} | {c["status_column"] for c in cells if c["kind"] == "state"})
    X = pd.read_parquet(pf["x_parquet"], columns=needed)
    X["session"] = X["session"].astype(str)
    # ── frozen input census BEFORE any outcome ──
    census = input_census(X, cells)
    build_rep = json.load(open(os.path.join(FAMILY_DIR, "runs", pf["x_run"], "build_report.json")))
    recon = reconcile_v2_1_census(census, cells, build_rep)
    census_p = run.write_atomic("input_census.json", dict(cells=census, v2_1_reconciliation=recon, x_sha256_16=pf["x_sha256_16"],
                                                          registry_sha256_16=pf["registry_sha256_16"], k=pf["k"]))
    census_sha = _dig(census_p)
    mkt_cuts = [float(x) for x in np.nanquantile(X["mkt_open1h"].to_numpy(float), [1 / 3, 2 / 3])]
    X["mkt_regime"] = pd.cut(X["mkt_open1h"], [-np.inf] + mkt_cuts + [np.inf], labels=["LOW", "MID", "HIGH"]).astype(object)
    rvl_bins = reg["v1_registry"]["features"]["OPEN1H_RVOL_LEVEL"][1]
    X["activity_bucket"] = None
    for b in rvl_bins:
        X.loc[bin_mask(X["open1h_rvol"], rvl_bins, b), "activity_bucket"] = b
    run.write_atomic("strata_binding.json", dict(mkt_regime_terciles_of_mkt_open1h=mkt_cuts, activity_bucket="OPEN1H_RVOL_LEVEL bins (V1) on open1h_rvol",
                                                 strata=["year", "price_bucket", "dv_bucket", "gap_bucket", "mkt_regime", "activity_bucket", "base_state"]))
    # ── outcome access: 0 -> 1, recorded once, immediately before the first _pathsim evaluation ──
    led = ledger_record_access(run.run_id, SEAL_V2_1_SHA256)
    first_access_at = led["events"][-1]["at"]
    log(f"OUTCOME ACCESS {led['outcome_access_count'] - 1} -> {led['outcome_access_count']} at {first_access_at}")
    xkeys = X[["ticker", "session"]]
    outcomes = {}
    for h in HORIZONS:
        t0 = time.time()
        o = row_outcomes(pf["derived_parquet"], xkeys, h, log)
        outcomes[h] = o
        p = os.path.join(run.dir, f"row_outcomes_h{h}.parquet"); o.to_parquet(p, index=False)
        log(f"  horizon {h}: {len(o):,} row outcomes in {time.time() - t0:.0f}s (censored {int(o['censored'].sum()):,})")
    # ── A10 distribution exposure flag on the PRIMARY horizon (actual entry / actual exit) ──
    a10 = json.load(open(os.path.join(CANON_DIR, "dividend_reference", "MATERIALITY_CENSUS_A10.json")))
    if _dig(a10["table"]) != a10["table_sha256_16"]:
        raise HardStop("A10 exposure set changed")
    ge5 = pd.read_parquet(a10["table"])[["ticker", "ex_date"]]; ge5["ex_date"] = ge5["ex_date"].astype(str)
    prim = outcomes[60].merge(ge5, on="ticker", how="left")
    prim["exp"] = (prim["ex_date"].notna()) & (prim["date_in"] < prim["ex_date"]) & (prim["ex_date"] <= prim["date_out"])
    exposed = prim.groupby(["ticker", "session"])["exp"].any().rename("dist_exposed").reset_index()
    outcomes[60] = outcomes[60].merge(exposed, on=["ticker", "session"], how="left")
    # ── per-cell statistics, horizon by horizon (one merged frame at a time) ──
    results = [dict(claim_id=c["claim_id"], kind=c["kind"], family=c["family"], feature=c["feature"], bin=c["bin"], logic=c["logic"],
                    resolution=c["resolution"], sample_authority=c["sample_authority"], as_of=c["as_of"], entry=c["entry"],
                    entry_offset_from_D=c["entry_offset_from_D"], **{k: census[c["claim_id"]][k] for k in ("eligible_n", "TRUE", "FALSE", "UNAVAILABLE")})
               for c in cells]
    day_series = {}
    for h in HORIZONS:
        Xh = X.merge(outcomes[h], on=["ticker", "session"], how="left")
        ctrl_cache = {}
        t0 = time.time()
        for ci, (c, rec) in enumerate(zip(cells, results)):
            ins, ev, tr = cell_state(Xh, c)
            defined = Xh["ret"].notna().to_numpy()
            key = (h, c["family"], tuple(c["columns"]))
            if key not in ctrl_cache:
                sub = Xh.loc[ev & defined, ["date_in", "ret"]]
                ctrl, noth = loo_control(sub)
                ctrl_cache[key] = (pd.Series(ctrl, index=sub.index), pd.Series(noth, index=sub.index))
            ctrl_s, noth_s = ctrl_cache[key]
            idx = Xh.index[tr & defined]
            obs = Xh.loc[idx, ["ticker", "date_in", "ret", "mae", "mfe", "hold", "censored", "price_bucket", "dv_bucket", "gap_bucket",
                               "mkt_regime", "activity_bucket", "base_state"] + (["dist_exposed"] if h == 60 else [])].copy()
            obs["control"] = ctrl_s.reindex(idx).to_numpy(); obs["n_others"] = noth_s.reindex(idx).fillna(0).to_numpy()
            obs["edge"] = np.where(obs["n_others"] >= MIN_CONTROL, obs["ret"] - obs["control"], np.nan)
            e = obs.dropna(subset=["edge"])
            ds = day_stats(e); ts = trade_stats(obs)
            tag = "" if h == 60 else f"_h{h}"
            rec.update({f"pathsim_defined_n{tag}": int(len(obs)), f"unmatched_n{tag}": int(obs["edge"].isna().sum()),
                        f"median_edge{tag}": ds["median_edge"], f"n_days{tag}": ds["n_days"], f"day_win{tag}": ds["day_win"],
                        f"raw_median{tag}": ts.get("raw_median")})
            if h == 60:
                rec.update(dict(top2_share=ds["top2_share"], per_year=ds["per_year"], days_per_year=ds["days_per_year"],
                                **{k: v for k, v in ts.items() if k != "n_obs"}, n_obs=ts["n_obs"]))
                yrs = {y: v for y, v in ds["per_year"].items() if ds["days_per_year"].get(y, 0) >= GATES["build"]["min_days_per_year"]}
                rec.update(dict(positive_years=sum(1 for v in yrs.values() if v > 0), years_counted=len(yrs),
                                worst_year=(min(yrs.values()) if yrs else None), best_year=(max(yrs.values()) if yrs else None)))
                strata = {}
                for s in ("price_bucket", "dv_bucket", "gap_bucket", "mkt_regime", "activity_bucket", "base_state"):
                    strata[s] = {}
                    for val, g in e.groupby(s, dropna=True):
                        d = g.groupby("date_in")["edge"].median()
                        strata[s][str(val)] = dict(n_days=int(len(d)), median_edge=round(float(d.median()), 3))
                rec["strata"] = strata
                dist = {}
                for lab, g in (("all", e), ("exposed", e[e["dist_exposed"] == True]), ("non_exposed", e[e["dist_exposed"] != True])):  # noqa: E712
                    d = g.groupby("date_in")["edge"].median() if len(g) else pd.Series(dtype=float)
                    dist[lab] = dict(n_obs=int(len(g)), n_days=int(len(d)), median_edge=(round(float(d.median()), 3) if len(d) else None))
                rec["distribution_diagnostic"] = dist
                day_series[c["claim_id"]] = ds.get("day_series", pd.Series(dtype=float))
            if (ci + 1) % 100 == 0:
                log(f"  horizon {h}: cells {ci + 1}/{len(cells)} ({time.time() - t0:.0f}s)")
        del Xh, ctrl_cache
    # ── multiplicity: overfit_stats.dsr, trial family = all 300 day-edge Sharpes, n_trials = 300 ──
    from overfit_stats import dsr, sharpe
    trial_srs = [sharpe(day_series[c["claim_id"]].to_numpy()) if len(day_series[c["claim_id"]]) >= 2 else 0.0 for c in cells]
    for rec, c in zip(results, cells):
        s = day_series[c["claim_id"]].to_numpy(float)
        if len(s) >= 3:
            dp = dsr(s, trial_srs, n_trials=pf["k"]); dn = dsr(-s, [-x for x in trial_srs], n_trials=pf["k"])
            rec.update(dsr=dp["dsr"], sr=dp["sr"], sr_star=dp["sr_star"], dsr_neg=dn["dsr"])
        else:
            rec.update(dsr=None, sr=None, sr_star=None, dsr_neg=None)
        rec["classification"] = classify(dict(n_days=rec["n_days"], n_obs=rec["n_obs"], median_edge=rec["median_edge"] or 0.0,
                                              day_win=rec["day_win"] or 0.0, per_year=rec["per_year"], days_per_year=rec["days_per_year"]),
                                         rec["dsr"] or 0.0, rec["dsr_neg"] or 0.0)
        rec["thin"] = rec["n_days"] < THIN_DAYS or rec["n_obs"] < THIN_OBS
    assert_result_reconciles([r["claim_id"] for r in results], cells)
    # cooldown-thinned direct cell-mask reconciliation for the seven V2.1 cells (descriptive)
    ps, _ = sacred_pathsim()
    direct = {}
    canon = pd.read_parquet(pf["derived_parquet"], columns=["ticker", "session_date", "open", "high", "low", "close", "atr_14"])
    canon = canon.rename(columns={"session_date": "date"}); canon["date"] = canon["date"].astype(str)
    for c in cells:
        if c["kind"] != "state":
            continue
        _, _, tr = cell_state(X, c)
        keys = set(zip(X.loc[tr, "ticker"], X.loc[tr, "session"]))
        grp = {}
        for tk, g in canon[canon["ticker"].isin({k for k, _ in keys})].groupby("ticker", sort=False):
            g = g.reset_index(drop=True); g["CELL"] = [(tk, d) in keys for d in g["date"]]; grp[tk] = g
        t = ps(grp, "CELL", PARAMS["mode"], PARAMS["stop"], PARAMS["target"], PARAMS["trail_fallback"], 60, slip=None, atr_k=PARAMS["atr_k"])
        direct[c["claim_id"]] = dict(rows_true=int(len(keys)), trades_taken_with_cooldown=int(len(t)),
                                     raw_median_taken=round(float(t["ret"].median() * 100), 3) if len(t) else None)
    # ── persist (atomic) + COMPLETED binding ──
    res_df = pd.DataFrame([{k: (json.dumps(v, default=str) if isinstance(v, (dict, list)) else v) for k, v in r.items()} for r in results])
    res_p = os.path.join(run.dir, "results_300.parquet"); res_df.to_parquet(res_p, index=False)
    run.write_atomic("results_300.json", results)
    cls = {}
    for r in results:
        cls[r["classification"]] = cls.get(r["classification"], 0) + 1
    params_sha = _jdig(PARAMS)
    summary = dict(run_id=run.run_id, seal_v2_1_sha256=SEAL_V2_1_SHA256, registry_sha256_16=pf["registry_sha256_16"], k=pf["k"],
                   x_run=pf["x_run"], x_sha256_16=pf["x_sha256_16"], canonical=pf["canonical"], pathsim_src_sha256_16=PATHSIM_SRC_SHA,
                   params=PARAMS, params_sha256_16=params_sha, horizons=HORIZONS, min_control=MIN_CONTROL, gates=GATES,
                   input_census_sha256_16=census_sha, first_outcome_access_at=first_access_at, outcome_access_count=led["outcome_access_count"],
                   reconciliation=dict(tested=len(results), missing=0, extra=0, duplicate=0),
                   row_outcomes={h: dict(n=int(len(outcomes[h])), censored=int(outcomes[h]["censored"].sum())) for h in HORIZONS},
                   dist_exposed_rows=int(outcomes[60]["dist_exposed"].fillna(False).sum()),
                   classification_census=cls, direct_cell_mask_reconciliation=direct, fixtures=pf["fixtures"],
                   completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summary)
    run.complete("results_300.parquet")
    run.write_atomic("STATUS.json", dict(run_id=run.run_id, status="OUTCOME_COMPLETED", seal_v2_1_sha256=SEAL_V2_1_SHA256,
                                         results_sha256_16=_dig(res_p), summary_sha256_16=_dig(os.path.join(run.dir, "summary.json"))))
    # ── FIRST-OUTCOME GOVERNANCE RECORD (immutable) ──
    gov = dict(record_id="OPENING_VOLUME_DYNAMICS_FIRST_OUTCOME_EXECUTION_V1", run_id=run.run_id,
               parent_seal_v2_1_sha256=SEAL_V2_1_SHA256, first_outcome_access_at=first_access_at, outcome_access_count=led["outcome_access_count"],
               pathsim=dict(path=os.path.join(HERE, "edge_replay.py"), src_sha256_16=PATHSIM_SRC_SHA, params=PARAMS, params_sha256_16=params_sha),
               registry_sha256_16=pf["registry_sha256_16"], k=pf["k"], input_census_sha256_16=census_sha,
               results_sha256_16=_dig(res_p), summary_sha256_16=_dig(os.path.join(run.dir, "summary.json")),
               fixtures=pf["fixtures"], harness_code_bound_in_v2_1=pf["harness_code"], runner_sha256_16=_dig(__file__),
               execution_design_decisions=["row-outcome table via five disjoint mod-5 masks so the engine's 5-bar cooldown never thins an observation (engine unmodified)",
                                           "control = leave-one-out median of family rows evaluable for the cell on the same entry date, >= 20 others",
                                           "mkt_regime terciles cut on the sealed X table at run start (strata_binding.json)",
                                           "THIN = n_days < 30 or n_obs < 100 (label only; stays in k)",
                                           "classification thresholds = registered analysis standard, written before any outcome"],
               disclosed_defects=[], v1_seal_sha256=pf["v1_seal"], v2_seal_sha256=pf["v2_seal"],
               status="COMPLETE — the family is now SEARCH-EXPOSED; any hypothesis from these outcomes is POST_EXPOSURE_HYPOTHESIS",
               written_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    gp = os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION_V1.json")
    if os.path.exists(gp):
        raise HardStop("FIRST_OUTCOME_EXECUTION_V1.json already exists — immutable")
    json.dump(gov, open(gp, "w"), indent=1, default=str)
    log(f"COMPLETED {run.run_id}; governance record {_dig(gp)}")
    return summary


if __name__ == "__main__":
    if "--preflight" in sys.argv:
        pf = preflight()
        print(json.dumps({k: v for k, v in pf.items() if k not in ("cells", "reg")}, indent=1, default=str))
        print("cells:", len(pf["cells"]))
    else:
        print(json.dumps(run(), indent=1, default=str))
