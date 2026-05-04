"""
Replay Analytics Engine
Reads stock_stat CSV, computes replay labels, runs all analytics sections.
Produces CSV reports and a markdown summary.
"""
import os, io, csv, json, zipfile, logging, itertools
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

REPLAY_OUTPUT_DIR = "replay_output"
STOCK_STAT_DIR    = "stock_stat_output"

# ─── Run state ─────────────────────────────────────────────────────────────────
_state: Dict[str, Any] = {
    "status":       "idle",   # idle / running / completed / failed
    "started_at":   None,
    "completed_at": None,
    "progress":     0,
    "total_steps":  18,
    "error":        None,
    "reports":      {},       # name → {rows, path, generated_at}
    "tf":           "1d",
    "universe":     "sp500",
    "row_count":    0,
    "message":      "",
}

def get_state() -> dict:
    return dict(_state)


# ─── Numeric helpers ───────────────────────────────────────────────────────────

def _f(v, default=0.0) -> float:
    try:
        f = float(v)
        return default if (f != f) else f   # NaN guard
    except (TypeError, ValueError):
        return default

def _mean(vals: list) -> Optional[float]:
    v = [x for x in vals if x is not None and x == x]
    return round(sum(v) / len(v), 4) if v else None

def _median(vals: list) -> Optional[float]:
    v = sorted(x for x in vals if x is not None and x == x)
    n = len(v)
    if not n:
        return None
    mid = n // 2
    return round((v[mid - 1] + v[mid]) / 2, 4) if n % 2 == 0 else round(v[mid], 4)

def _rate(bools: list) -> Optional[float]:
    return round(sum(1 for b in bools if b) / len(bools), 4) if bools else None

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── CSV helpers ───────────────────────────────────────────────────────────────

def _write_csv(path: str, rows: List[dict], generated_at: str = "") -> int:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write(f"generated_at\n{generated_at}\n")
        return 0
    cols = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return len(rows)


def _load_stock_stat(tf="1d", universe="sp500") -> Tuple[Optional[List[dict]], Optional[str]]:
    # nasdaq_1 / nasdaq_2 are virtual halves of the full nasdaq file
    batch: Optional[int] = None
    actual_universe = universe
    if universe == "nasdaq_1":
        actual_universe, batch = "nasdaq", 1
    elif universe == "nasdaq_2":
        actual_universe, batch = "nasdaq", 2

    path = os.path.join(STOCK_STAT_DIR, f"stock_stat_{actual_universe}_{tf}.csv")
    if not os.path.exists(path):
        return None, f"File not found: {path}. Run Stock Stat first."
    rows = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append(dict(row))
    except Exception as e:
        return None, str(e)

    if batch is not None:
        tickers = sorted({r.get("ticker", r.get("Ticker", "")) for r in rows})
        mid = len(tickers) // 2
        half = set(tickers[:mid] if batch == 1 else tickers[mid:])
        rows = [r for r in rows if r.get("ticker", r.get("Ticker", "")) in half]
        log.info("nasdaq batch=%d: %d of %d tickers (%d rows)",
                 batch, len(half), len(tickers), len(rows))

    return rows, None


# ─── Row accessors (CSV columns are mixed-case) ────────────────────────────────

def _n(r: dict, *keys) -> float:
    """Get first matching numeric value from row dict."""
    for k in keys:
        v = r.get(k)
        if v is not None and v != "":
            f = _f(v)
            if f != 0.0 or v == "0" or v == "0.0":
                return f
    return 0.0

def _str(r: dict, *keys) -> str:
    for k in keys:
        v = r.get(k)
        if v is not None:
            return str(v)
    return ""

def _bool(r: dict, *keys) -> bool:
    for k in keys:
        v = r.get(k)
        if v is not None:
            return bool(int(_f(v, 0)))
    return False


# ─── Signal columns ─────────────────────────────────────────────────────────────

_ALL_SIG_COLS = [
    "SIG_BEST","SIG_STRONG","SIG_VBO_DN","SIG_NS_VABS","SIG_ND_VABS",
    "SIG_SC","SIG_BC","SIG_ABS","SIG_CLM",
    "SIG_BEST_UP","SIG_FBO_UP","SIG_EB_UP","SIG_3UP",
    "SIG_FBO_DN","SIG_EB_DN","SIG_4BF_DN",
    "SIG_FRI34","SIG_FRI43","SIG_FRI64",
    "SIG_L555","SIG_L2L4","SIG_BLUE",
    "SIG_CCI","SIG_CCI0R","SIG_CCIB",
    "SIG_BO_DN","SIG_BX_DN","SIG_BE_DN",
    "SIG_RL","SIG_RH","SIG_PP",
    "SIG_G1","SIG_G2","SIG_G4","SIG_G6","SIG_G11",
    "SIG_B1","SIG_B2","SIG_B3","SIG_B4","SIG_B5","SIG_B6",
    "SIG_B7","SIG_B8","SIG_B9","SIG_B10","SIG_B11",
    "SIG_F1","SIG_F2","SIG_F3","SIG_F4","SIG_F5","SIG_F6",
    "SIG_F7","SIG_F8","SIG_F9","SIG_F10","SIG_F11",
    "SIG_FLY_ABCD","SIG_FLY_CD","SIG_FLY_BD","SIG_FLY_AD",
    "SIG_WK_UP","SIG_WK_DN","SIG_X1","SIG_X2","SIG_X1G","SIG_X3",
    "SIG_BIAS_UP","SIG_BIAS_DN","SIG_SVS","SIG_CONSO",
    "SIG_P2","SIG_P3","SIG_P50","SIG_P89","SIG_BUY","SIG_3G",
    "SIG_VA","SIG_VOL_5X","SIG_VOL_10X","SIG_VOL_20X",
    "SIG_TZ","SIG_T","SIG_Z","SIG_TZ3","SIG_TZ2","SIG_TZ_FLIP",
    "SIG_CD","SIG_CA","SIG_CW","SIG_SEQ_BCONT",
    "SIG_NS_DELTA","SIG_ND_DELTA",
    "SIG_ANY_F","SIG_ANY_B","SIG_ANY_P","SIG_ANY_D",
    "SIG_L_ANY","SIG_BE_ANY","SIG_GOG_PLUS","SIG_NOT_EXT",
    "PRICE_GT_20","PRICE_GT_50","PRICE_GT_89","PRICE_GT_200",
    "PRICE_LT_20","PRICE_LT_50","PRICE_LT_89","PRICE_LT_200",
    "RSI_LE_35","RSI_GE_70",
    "SIG_P66","SIG_P55",
    "SIG_D66","SIG_D55","SIG_D89","SIG_D50","SIG_D3","SIG_D2",
    "SIG_FLP_UP","SIG_ORG_UP","SIG_DD_UP_RED","SIG_D_UP_RED",
    "SIG_D_DN_GREEN","SIG_DD_DN_GREEN",
    "SIG_CISD_CPLUS","SIG_CISD_CPLUS_MINUS","SIG_CISD_CPLUS_MM",
    "SIG_PARA_PREP","SIG_PARA_START","SIG_PARA_PLUS","SIG_PARA_RETEST",
    # Context tokens
    "LD","LDS","LDC","LDP","LRC","LRP","WRC","F8C","SQB","BCT","SVS",
    # GOG tiers
    "G1P","G2P","G3P","G1L","G2L","G3L","G1C","G2C","G3C","GOG1","GOG2","GOG3",
]

# Signals actively scored (used for "unscored" detection)
_SCORED = {
    "SIG_3UP","SIG_BLUE","SIG_B6","SIG_B8","SIG_F3","SIG_F4","SIG_F11",
    "SIG_F8","SIG_F6","SIG_L555","SIG_FRI64","SIG_FRI43","SIG_FRI34",
    "SIG_CCI0R","SIG_CCIB","SIG_P50",
    "SIG_PARA_PLUS","SIG_PARA_START","SIG_PARA_PREP","SIG_PARA_RETEST",
    "SIG_CISD_CPLUS","SIG_CISD_CPLUS_MINUS",
    "SIG_FLY_ABCD","SIG_FLY_CD","SIG_FLY_BD","SIG_FLY_AD",
    "SIG_D50","SIG_D2","SIG_D55","SIG_D66","SIG_D89","SIG_D3",
    "SIG_BIAS_UP","SIG_BIAS_DN","SIG_BC",
    "SIG_BE_DN","SIG_EB_DN","SIG_4BF_DN","SIG_WK_DN",
    "SIG_FBO_DN","SIG_RH","SIG_ND_VABS",
    "SIG_D_DN_GREEN","SIG_DD_DN_GREEN","SIG_ND_DELTA",
    "SIG_BEST_UP","SIG_FLP_UP","SIG_ORG_UP","SIG_DD_UP_RED","SIG_D_UP_RED",
    "SIG_SEQ_BCONT","SIG_VOL_5X","SIG_VOL_10X","SIG_VOL_20X",
    "SIG_P66","SIG_P55",
    "BCT","SQB","LRP","LDP","LRC","WRC","F8C","SVS",
    "G1P","G1C","G1L","GOG1","G2C","G2L","GOG2","G3C","G3L","GOG3",
    "PRICE_GT_200","PRICE_GT_89","PRICE_GT_50","RSI_LE_35","RSI_GE_70",
}

_MODEL_COLS = [
    "MDL_UM_GOG1","MDL_BH_GOG1","MDL_F8_GOG1","MDL_F8_BCT","MDL_F8_LRP",
    "MDL_L22_BCT","MDL_L22_LRP","MDL_BE_GOG1","MDL_BO_GOG1","MDL_Z10_GOG1",
    "MDL_LOAD_GOG1","MDL_260_GOG1","MDL_RKT_GOG1","MDL_F8_SVS","MDL_F8_CONS",
    "MDL_L22_SQB","MDL_3UP_GOG1","MDL_BLUE_GOG1","MDL_BX_GOG1","MDL_UM_LRP",
    # Rebound models (v4.4)
    "MDL_TZ_FLIP_Z","MDL_TZ_FLIP_WKUP","MDL_TZ3_VBO_DN",
    "MDL_ABS_RL","MDL_ABS_RH","MDL_BLUE_EBUP",
    "MDL_BEANY_F7_NDDELTA","MDL_CA_WKDN_UNDER50",
    "MDL_CA_NDVABS_UNDER50","MDL_RH_TZ3_UNDER200",
    "MDL_BEANY_BODN_NDDELTA",
    # Rocket models (v4.4)
    "MDL_SC_UM","MDL_SC_VOL5X","MDL_PARA_PLUS_UM",
    "MDL_PARA_START_UM","MDL_PARA_RETEST_UM",
    # Aggregated flags
    "HAS_ELITE_MODEL","HAS_BEAR_MODEL",
    "HAS_REBOUND_MODEL","HAS_STRONG_BULL_MODEL","HAS_HARD_BEAR_MODEL",
]

# Current score weights per signal (approximate, for "scored weak" analysis)
_SIG_WEIGHTS = {
    "SIG_PARA_PLUS": ("ROCKET_SCORE", 25),
    "SIG_PARA_START": ("ROCKET_SCORE", 15),
    "SIG_VOL_20X": ("ROCKET_SCORE", 10),
    "SIG_VOL_10X": ("ROCKET_SCORE", 6),
    "SIG_SEQ_BCONT": ("ROCKET_SCORE", 8),
    "SIG_F8": ("CLEAN_ENTRY_SCORE", 12),
    "SIG_F6": ("CLEAN_ENTRY_SCORE", 10),
    "SIG_3UP": ("CLEAN_ENTRY_SCORE", 8),
    "SIG_BLUE": ("CLEAN_ENTRY_SCORE", 6),
    "SIG_B6": ("CLEAN_ENTRY_SCORE", 5),
    "SIG_B8": ("CLEAN_ENTRY_SCORE", 5),
    "SIG_F3": ("CLEAN_ENTRY_SCORE", 5),
    "SIG_F4": ("CLEAN_ENTRY_SCORE", 5),
    "SIG_F11": ("CLEAN_ENTRY_SCORE", 5),
    "SIG_D50": ("EXTRA_BULL_SCORE", 6),
    "SIG_D2": ("EXTRA_BULL_SCORE", 6),
    "SIG_D55": ("EXTRA_BULL_SCORE", 5),
    "SIG_BIAS_UP": ("EXTRA_BULL_SCORE", 5),
    "SIG_L555": ("EXTRA_BULL_SCORE", 6),
    "SIG_FRI64": ("EXTRA_BULL_SCORE", 5),
    "SIG_P50": ("EXTRA_BULL_SCORE", 5),
    "SIG_BEST_UP": ("EXTRA_BULL_SCORE", 8),
    "SIG_CISD_CPLUS": ("EXPERIMENTAL_SCORE", 8),
    "SIG_FLY_ABCD": ("EXPERIMENTAL_SCORE", 8),
    "SIG_FLY_CD": ("EXPERIMENTAL_SCORE", 5),
    "SIG_PARA_PREP": ("EXPERIMENTAL_SCORE", 5),
    "SIG_BE_DN": ("SHAKEOUT_ABSORB_SCORE", 18),
    "SIG_EB_DN": ("SHAKEOUT_ABSORB_SCORE", 15),
    "SIG_4BF_DN": ("SHAKEOUT_ABSORB_SCORE", 12),
    "SIG_WK_DN": ("SHAKEOUT_ABSORB_SCORE", 10),
    "SIG_FBO_DN": ("HARD_BEAR_SCORE", 20),
    "SIG_BIAS_DN": ("HARD_BEAR_SCORE", 6),
    "SIG_BC": ("HARD_BEAR_SCORE", 6),
    "SIG_RH": ("HARD_BEAR_SCORE", 10),
    "SIG_ND_VABS": ("HARD_BEAR_SCORE", 8),
    "SIG_D_DN_GREEN": ("HARD_BEAR_SCORE", 12),
    "SIG_DD_DN_GREEN": ("HARD_BEAR_SCORE", 15),
    "SIG_ND_DELTA": ("HARD_BEAR_SCORE", 10),
    # v4.4 rebound signals
    "SIG_TZ_FLIP": ("REBOUND_SQUEEZE_SCORE", 8),
    "SIG_WK_UP": ("REBOUND_SQUEEZE_SCORE", 6),
    "SIG_RL": ("REBOUND_SQUEEZE_SCORE", 5),
    "SIG_CA": ("REBOUND_SQUEEZE_SCORE", 5),
}


def _active_sigs(row: dict, cols=None) -> List[str]:
    return [c for c in (cols or _ALL_SIG_COLS) if c in row and _f(row[c]) > 0]


def _nonzero_cols(rows: List[dict], cols: List[str]) -> List[str]:
    present = set()
    for r in rows:
        for c in cols:
            if c in r and _f(r.get(c, 0)) > 0:
                present.add(c)
    return [c for c in cols if c in present]


# ─── Missed winner classification (prior-lookback, no future data) ────────────

_ACTIONABLE_SCORE    = 60   # bull score >= this = actionable prior entry
_PRIOR_LOOKBACK_BARS = 20   # trading sessions to look back per ticker

# Regimes that are NOT actionable (too bearish / neutral to enter)
_NON_ACTIONABLE_REGIMES = {"BEARISH_PHASE", "NEUTRAL_OR_LOW", "NONE", ""}

# Model columns that indicate an actionable named model is present
_ACTIONABLE_MODEL_COLS = ["HAS_ELITE_MODEL", "HAS_REBOUND_MODEL", "HAS_STRONG_BULL_MODEL"]


# ── Canonical bull-score reader ────────────────────────────────────────────
# FINAL_BULL_SCORE is now written to stock_stat CSV by canonical_scoring_engine.
# It equals turbo_score (both computed from the same _calc_turbo_score call).
# This reader accepts either column name; stock_stat CSV always has FINAL_BULL_SCORE
# after the canonical engine upgrade.
def _bull_score(r: dict) -> float:
    v = r.get("FINAL_BULL_SCORE")
    if v not in (None, ""):
        f = _f(v)
        if f != 0.0 or v in ("0", "0.0"):
            return f
    return _f(r.get("turbo_score", 0))


def _is_prior_actionable(r: dict) -> bool:
    """True if a prior row represents an actionable entry opportunity."""
    if _bull_score(r) >= _ACTIONABLE_SCORE:
        return True
    regime = _str(r, "FINAL_REGIME")
    if regime and regime not in _NON_ACTIONABLE_REGIMES:
        return True
    if any(_f(r.get(m, 0)) > 0 for m in _ACTIONABLE_MODEL_COLS):
        return True
    return False


def _days_between(d1: str, d2: str) -> int:
    try:
        from datetime import date as _date
        return (_date.fromisoformat(d2) - _date.fromisoformat(d1)).days
    except Exception:
        return 0


def _best_score_in_window(trows: list, i: int, n: int) -> float:
    return max((_bull_score(p) for p in trows[max(0, i - n):i]),
               default=0.0)


def _validate_score_source(rows: List[dict]) -> Optional[str]:
    """
    Validate that stock_stat was generated with the canonical scoring engine
    (canonical_scoring_engine.py). After the upgrade, FINAL_BULL_SCORE and
    FINAL_REGIME are written to the CSV alongside turbo_score.
    If these columns are absent the CSV was built with old code → warn to re-run.
    """
    if not rows:
        return None
    first = rows[0]
    has_score = "FINAL_BULL_SCORE" in first or "turbo_score" in first
    if not has_score:
        return (
            "stock_stat CSV is missing both FINAL_BULL_SCORE and turbo_score. "
            "Re-run Stock Stat scan to regenerate with the canonical scoring engine."
        )
    # Soft warning: canonical columns absent means old stock_stat
    missing_canonical = [c for c in ("FINAL_BULL_SCORE", "FINAL_REGIME", "ROCKET_SCORE")
                         if c not in first]
    if missing_canonical:
        log.warning(
            "stock_stat missing canonical columns %s — re-run Stock Stat for full analytics. "
            "Replay will use turbo_score as FINAL_BULL_SCORE fallback.",
            missing_canonical,
        )
    return None


def _classify_missed_winners(rows: List[dict]) -> None:
    """
    Classify ALL missed winners using prior-lookback of the same ticker's score history.
    Categories are mutually exclusive and exhaustive — every missed row gets exactly one.

    TRUE_MISSED_WINNER   — BIG_WIN_10D=True, FINAL_BULL_SCORE<60, not extended,
                           AND no prior actionable score/regime/model in the 20 trading
                           sessions before this date for this ticker.
    CAUGHT_EARLY_WINNER  — BIG_WIN_10D=True, FINAL_BULL_SCORE<60, not extended,
                           AND at least one prior row within 20 sessions was actionable
                           (score>=60 OR bullish regime OR named model present).
    LATE_OR_WEAK_CATCH   — BIG_WIN_10D=True, FINAL_BULL_SCORE<60,
                           AND already_extended=True at signal time.

    Classification is prior-lookback only — no GOG/VBO timing, no future data.
    SNDK 2026-03-30 → CAUGHT_EARLY because 03-03(86), 03-06(64), 03-25(92),
    03-26(105) are all actionable prior entries within 20 sessions.
    """
    from collections import defaultdict

    # ── Step 1: initialize all prior-catch fields to defaults ──────────────────
    for r in rows:
        r["_prior_actionable"]     = False
        r["_best_prior_score_3d"]  = 0.0
        r["_best_prior_score_5d"]  = 0.0
        r["_best_prior_score_10d"] = 0.0
        r["_best_prior_score_20d"] = 0.0
        r["_best_prior_date_20d"]  = ""
        r["_best_prior_regime_20d"]= ""
        r["_days_since_best_prior"]= 0
        if not r.get("_missed"):
            r["_missed_cat"] = ""

    # ── Step 2: classify ticker-by-ticker using date-sorted history ────────────
    ticker_rows: Dict[str, List[dict]] = defaultdict(list)
    for r in rows:
        t = _str(r, "ticker")
        if t:
            ticker_rows[t].append(r)

    for trows in ticker_rows.values():
        try:
            trows.sort(key=lambda r: _str(r, "date"))
        except Exception:
            pass

        for i, r in enumerate(trows):
            if not r.get("_missed"):
                continue  # already set to "" in step 1

            window   = trows[max(0, i - _PRIOR_LOOKBACK_BARS):i]
            cur_date = _str(r, "date")

            r["_best_prior_score_3d"]  = _best_score_in_window(trows, i, 3)
            r["_best_prior_score_5d"]  = _best_score_in_window(trows, i, 5)
            r["_best_prior_score_10d"] = _best_score_in_window(trows, i, 10)
            r["_best_prior_score_20d"] = _best_score_in_window(trows, i, 20)

            best_row = (max(window, key=lambda p: _bull_score(p))
                        if window else None)
            r["_best_prior_date_20d"]   = _str(best_row, "date")         if best_row else ""
            r["_best_prior_regime_20d"] = _str(best_row, "FINAL_REGIME") if best_row else ""
            r["_days_since_best_prior"] = (_days_between(r["_best_prior_date_20d"], cur_date)
                                           if r["_best_prior_date_20d"] else 0)

            had_prior = any(_is_prior_actionable(p) for p in window)
            r["_prior_actionable"] = had_prior

            # Exactly one branch fires — mutually exclusive by construction
            if r["_ext"]:
                r["_missed_cat"] = "LATE_OR_WEAK_CATCH"
            elif had_prior:
                r["_missed_cat"] = "CAUGHT_EARLY_WINNER"
            else:
                r["_missed_cat"] = "TRUE_MISSED_WINNER"

    # ── Step 3: catch any missed rows that had no ticker (fallback) ─────────────
    # Rows without a ticker string were skipped above; classify as LATE to be safe.
    for r in rows:
        if r.get("_missed") and not r.get("_missed_cat"):
            r["_missed_cat"] = "LATE_OR_WEAK_CATCH"
            log.warning("Row with empty ticker classified as LATE: %s %s",
                        r.get("ticker", ""), r.get("date", ""))


# ─── Regression test cases ─────────────────────────────────────────────────
#
# Stock Stat, Superchart, and Replay all read the SAME canonical column
# (`turbo_score`) produced by turbo_engine._calc_turbo_score. There is no
# separate per-ticker scoring path to cross-check against, so per-ticker
# regression cases are not used. Add real reference values here only after
# verifying them against the live per-ticker (Superchart) export.

_SCORE_REGRESSION_CASES: Dict[Tuple[str, str], float] = {}
_CLASSIFICATION_REGRESSION_CASES: List[Tuple[str, str, str]] = []

# Tickers/dates to validate in score consistency check.
# Scores are fetched live from the canonical engine (api_bar_signals) and
# compared against what's in stock_stat so any drift is caught at run time.
_CONSISTENCY_TICKERS = ["SNDK", "INTC"]
_CONSISTENCY_DATES = {
    "SNDK": ["2026-03-03", "2026-03-06", "2026-03-10",
             "2026-03-25", "2026-03-26", "2026-03-30",
             "2026-04-01", "2026-04-02"],
    "INTC": ["2026-03-25", "2026-03-26", "2026-03-30",
             "2026-04-01", "2026-04-02", "2026-04-22"],
}


def _score_consistency_check(rows: List[dict], tf: str = "1d") -> Tuple[List[dict], dict]:
    """
    Validate three-way consistency for SNDK and INTC reference dates:
      1. canonical (live api_bar_signals, uppercase keys)  — source of truth
      2. stock_stat CSV rows                               — what replay reads
      3. per-ticker export keys (lowercase keys on live bar) — what SuperchartPanel.jsx reads

    Returns (check_rows, export_check_rows, summary_dict).
    Callers unpack the first two lists and write them to separate CSVs.
    Hard-fails (status="fail") if any exported FINAL_BULL_SCORE is 0/empty
    while the canonical score is nonzero.
    """
    try:
        from main import api_bar_signals  # import here to avoid circular at module load
    except Exception as e:
        log.warning("score_consistency_check: cannot import api_bar_signals: %s", e)
        return [], [], {"status": "not_run", "mismatch_count": 0, "mismatches": []}

    by_key = {(_str(r, "ticker"), _str(r, "date")): r for r in rows}
    check_rows: List[dict]        = []
    export_check_rows: List[dict] = []
    mismatches: List[str]         = []

    for ticker in _CONSISTENCY_TICKERS:
        dates = _CONSISTENCY_DATES.get(ticker, [])
        if not dates:
            continue
        try:
            live_bars = api_bar_signals(ticker, tf, 150)
        except Exception as e:
            log.warning("score_consistency_check: fetch %s failed: %s", ticker, e)
            continue

        live_by_date = {b.get("date", ""): b for b in live_bars}

        for date in dates:
            live = live_by_date.get(date)
            stat = by_key.get((ticker, date))

            # ── Canonical (uppercase) — source of truth ─────────────────────
            canonical_score  = _f(live.get("FINAL_BULL_SCORE", 0))  if live else None
            canonical_regime = live.get("FINAL_REGIME", "")          if live else None
            canonical_bucket = live.get("FINAL_SCORE_BUCKET", "")    if live else None

            # ── Stock-stat CSV ──────────────────────────────────────────────
            stat_score  = _bull_score(stat)             if stat else None
            stat_regime = _str(stat, "FINAL_REGIME")    if stat else None
            stat_bucket = _str(stat, "FINAL_SCORE_BUCKET") if stat else None

            # ── Per-ticker export (lowercase keys — what SuperchartPanel reads) ─
            exported_score  = _f(live.get("final_bull_score", 0))  if live else None
            exported_regime = live.get("final_regime", "")          if live else None
            exported_bucket = live.get("final_score_bucket", "")    if live else None

            # ── Status for stock_stat vs canonical ──────────────────────────
            if live is None:
                status = "MISSING_LIVE_ROW"
            elif stat is None:
                status = "MISSING_STAT_ROW"
            else:
                score_diff = abs((canonical_score or 0) - (stat_score or 0))
                regime_ok  = canonical_regime == stat_regime
                if score_diff > 1 or not regime_ok:
                    status = "MISMATCH"
                    mismatches.append(
                        f"{ticker} {date}: canonical={canonical_score}/{canonical_regime} "
                        f"stat={stat_score}/{stat_regime} diff={score_diff:.1f}"
                    )
                else:
                    status = "OK"

            # ── Status for per-ticker export vs canonical ────────────────────
            if live is None:
                export_status = "MISSING_LIVE_ROW"
            else:
                canonical_nz = (canonical_score or 0) != 0
                exported_zero = (exported_score or 0) == 0
                if canonical_nz and exported_zero:
                    export_status = "EXPORT_ZERO_BUG"
                    mismatches.append(
                        f"{ticker} {date}: exported_final_bull_score=0 but canonical={canonical_score} "
                        f"— lowercase key missing from api_bar_signals response"
                    )
                elif exported_score != canonical_score or exported_regime != canonical_regime:
                    export_status = "EXPORT_MISMATCH"
                    mismatches.append(
                        f"{ticker} {date}: exported={exported_score}/{exported_regime} "
                        f"canonical={canonical_score}/{canonical_regime}"
                    )
                else:
                    export_status = "OK"

            check_rows.append({
                "ticker":                        ticker,
                "date":                          date,
                "status":                        status,
                "live_final_bull_score":         canonical_score,
                "stat_final_bull_score":         stat_score,
                "score_diff":                    round(abs((canonical_score or 0) - (stat_score or 0)), 1)
                                                 if canonical_score is not None and stat_score is not None else "",
                "live_final_regime":             canonical_regime,
                "stat_final_regime":             stat_regime,
                "regime_match":                  int(canonical_regime == stat_regime)
                                                 if canonical_regime is not None and stat_regime is not None else "",
                "live_final_score_bucket":       canonical_bucket,
                "stat_final_score_bucket":       stat_bucket,
                "live_rocket_score":             _f(live.get("ROCKET_SCORE", 0)) if live else "",
                "stat_rocket_score":             _n(stat, "ROCKET_SCORE") if stat else "",
                "live_clean_entry_score":        _f(live.get("CLEAN_ENTRY_SCORE", 0)) if live else "",
                "stat_clean_entry_score":        _n(stat, "CLEAN_ENTRY_SCORE") if stat else "",
                "live_hard_bear_score":          _f(live.get("HARD_BEAR_SCORE", 0)) if live else "",
                "stat_hard_bear_score":          _n(stat, "HARD_BEAR_SCORE") if stat else "",
                "live_rebound_squeeze_score":    _f(live.get("REBOUND_SQUEEZE_SCORE", 0)) if live else "",
                "stat_rebound_squeeze_score":    _n(stat, "REBOUND_SQUEEZE_SCORE") if stat else "",
            })

            export_check_rows.append({
                "ticker":                     ticker,
                "date":                       date,
                "status":                     export_status,
                "exported_final_bull_score":  exported_score,
                "canonical_final_bull_score": canonical_score,
                "score_match":                int(exported_score == canonical_score)
                                              if exported_score is not None and canonical_score is not None else "",
                "exported_final_regime":      exported_regime,
                "canonical_final_regime":     canonical_regime,
                "regime_match":               int(exported_regime == canonical_regime)
                                              if exported_regime is not None and canonical_regime is not None else "",
                "exported_final_score_bucket":  exported_bucket,
                "canonical_final_score_bucket": canonical_bucket,
                "bucket_match":               int(exported_bucket == canonical_bucket)
                                              if exported_bucket is not None and canonical_bucket is not None else "",
            })

    hard_fail_statuses = {"EXPORT_ZERO_BUG", "EXPORT_MISMATCH", "MISMATCH"}
    mismatch_count = sum(1 for r in check_rows + export_check_rows
                         if r.get("status") in hard_fail_statuses)
    summary = {
        "status": "ok" if mismatch_count == 0 else "fail",
        "mismatch_count": mismatch_count,
        "mismatches": mismatches,
    }
    return check_rows, export_check_rows, summary


def _clean_output_dir() -> int:
    """Remove stale replay_* files from REPLAY_OUTPUT_DIR before run."""
    if not os.path.exists(REPLAY_OUTPUT_DIR):
        return 0
    n = 0
    for fname in os.listdir(REPLAY_OUTPUT_DIR):
        if fname.startswith("replay_") and (fname.endswith(".csv") or fname.endswith(".md")):
            try:
                os.remove(os.path.join(REPLAY_OUTPUT_DIR, fname))
                n += 1
            except Exception as e:
                log.warning("Could not remove stale %s: %s", fname, e)
    return n


def _validate_score_samples(rows: List[dict]) -> Optional[str]:
    """
    Cross-check stock_stat scores against per-ticker export reference values.
    Fails the run if any sampled (ticker, date) score diverges by more than 5 points.
    """
    by_key = {(_str(r, "ticker"), _str(r, "date")): r for r in rows}
    mismatches = []
    for (ticker, date), expected in _SCORE_REGRESSION_CASES.items():
        r = by_key.get((ticker, date))
        if r is None:
            continue  # row not in this dataset (different universe/tf)
        actual = _bull_score(r)
        if abs(actual - expected) > 5:
            mismatches.append(
                f"{ticker} {date}: stock_stat={actual:.0f}, per-ticker_export={expected}"
            )
    if mismatches:
        return (
            "Replay scoring mismatch detected. Reports are stale or using a different "
            "scoring function. Sampled mismatches: " + "; ".join(mismatches) +
            ". Re-run Stock Stat scan so it uses the same scoring source as per-ticker export."
        )
    return None


def _validate_classification_regressions(rows: List[dict]) -> Optional[str]:
    """Fail the run if a known regression case is misclassified."""
    by_key = {(_str(r, "ticker"), _str(r, "date")): r for r in rows}
    failures = []
    for ticker, date, expected_cat in _CLASSIFICATION_REGRESSION_CASES:
        r = by_key.get((ticker, date))
        if r is None or not r.get("_missed"):
            continue
        actual_cat = r.get("_missed_cat", "")
        if actual_cat != expected_cat:
            failures.append(f"{ticker} {date}: expected={expected_cat}, got={actual_cat}")
    if failures:
        return ("Classification regression failed: " + "; ".join(failures) +
                ". The CAUGHT_EARLY classification depends on prior scores >= 60. "
                "If prior scores are wrong, the upstream stock_stat CSV is stale.")
    return None


def _validate_categories(rows: List[dict]) -> Optional[str]:
    """
    Verify that categories are mutually exclusive and exhaustive.
    Returns an error string on failure, None on success.
    Checks:
      category_sum == total_missed
      overlap_count == 0
    """
    missed = [r for r in rows if r.get("_missed")]
    total  = len(missed)

    true_n   = sum(1 for r in missed if r.get("_missed_cat") == "TRUE_MISSED_WINNER")
    caught_n = sum(1 for r in missed if r.get("_missed_cat") == "CAUGHT_EARLY_WINNER")
    late_n   = sum(1 for r in missed if r.get("_missed_cat") == "LATE_OR_WEAK_CATCH")
    cat_sum  = true_n + caught_n + late_n

    errors = []
    if cat_sum != total:
        uncategorized = total - cat_sum
        errors.append(
            f"category_sum({cat_sum}) != total_missed({total}); "
            f"uncategorized_rows={uncategorized}"
        )

    def _keys(cat: str):
        return {(_str(r, "ticker"), _str(r, "date"))
                for r in missed if r.get("_missed_cat") == cat}

    tc = _keys("TRUE_MISSED_WINNER") & _keys("CAUGHT_EARLY_WINNER")
    tl = _keys("TRUE_MISSED_WINNER") & _keys("LATE_OR_WEAK_CATCH")
    cl = _keys("CAUGHT_EARLY_WINNER") & _keys("LATE_OR_WEAK_CATCH")
    overlap = len(tc) + len(tl) + len(cl)
    if overlap:
        errors.append(f"overlap_count={overlap} (TC={len(tc)}, TL={len(tl)}, CL={len(cl)})")

    if errors:
        return ("Replay validation failed: " + "; ".join(errors) +
                ". Re-run Stock Stat or check classification logic.")
    return None


# ─── Forward-return computation (from OHLCV in stock_stat CSV) ────────────────

def _compute_forward_returns(rows: List[dict]) -> None:
    """
    Compute forward return labels from OHLCV data already in stock_stat CSV.
    Mutates rows in-place, adding:
        RET_1D, RET_3D, RET_5D, RET_10D   — close-to-close % return
        MAX_RET_5D, MAX_RET_10D            — max intrabar high % vs entry close
        ALREADY_EXTENDED_FLAG              — 1 if price is >20% above 10-bar low

    Called before _label_rows() so these fields are available for labeling.
    Rows that are near the end of the dataset (no future bars) get 0 for returns.
    """
    from collections import defaultdict

    # Group by ticker, sort by date
    by_ticker: Dict[str, List[dict]] = defaultdict(list)
    for r in rows:
        t = _str(r, "ticker")
        if t:
            by_ticker[t].append(r)

    for trows in by_ticker.values():
        trows.sort(key=lambda r: _str(r, "date"))
        n = len(trows)
        for i, r in enumerate(trows):
            c0 = _f(r.get("close", 0))
            if c0 <= 0:
                r["RET_1D"] = 0.0
                r["RET_3D"] = 0.0
                r["RET_5D"] = 0.0
                r["RET_10D"] = 0.0
                r["MAX_RET_5D"] = 0.0
                r["MAX_RET_10D"] = 0.0
                r["ALREADY_EXTENDED_FLAG"] = 0
                continue

            def _fwd_close(k):
                j = i + k
                if j < n:
                    return _f(trows[j].get("close", 0))
                return 0.0

            def _fwd_high(lo, hi):
                highs = []
                for k in range(lo, min(hi + 1, n - i)):
                    h = _f(trows[i + k].get("high", 0))
                    if h > 0:
                        highs.append(h)
                return max(highs) if highs else 0.0

            c1  = _fwd_close(1)
            c3  = _fwd_close(3)
            c5  = _fwd_close(5)
            c10 = _fwd_close(10)

            r["RET_1D"]  = round((c1  / c0 - 1) * 100, 4) if c1  > 0 else 0.0
            r["RET_3D"]  = round((c3  / c0 - 1) * 100, 4) if c3  > 0 else 0.0
            r["RET_5D"]  = round((c5  / c0 - 1) * 100, 4) if c5  > 0 else 0.0
            r["RET_10D"] = round((c10 / c0 - 1) * 100, 4) if c10 > 0 else 0.0

            max_h5  = _fwd_high(1, 5)
            max_h10 = _fwd_high(1, 10)
            r["MAX_RET_5D"]  = round((max_h5  / c0 - 1) * 100, 4) if max_h5  > 0 else 0.0
            r["MAX_RET_10D"] = round((max_h10 / c0 - 1) * 100, 4) if max_h10 > 0 else 0.0

            # ALREADY_EXTENDED: close > 20% above the lowest close in prior 10 bars
            prior_closes = [_f(trows[j].get("close", 0))
                            for j in range(max(0, i - 10), i)
                            if _f(trows[j].get("close", 0)) > 0]
            if prior_closes:
                low10 = min(prior_closes)
                r["ALREADY_EXTENDED_FLAG"] = int(low10 > 0 and (c0 / low10 - 1) > 0.20)
            else:
                r["ALREADY_EXTENDED_FLAG"] = 0

    # Rows with no ticker get zero returns
    for r in rows:
        if not _str(r, "ticker"):
            for col in ("RET_1D", "RET_3D", "RET_5D", "RET_10D",
                        "MAX_RET_5D", "MAX_RET_10D", "ALREADY_EXTENDED_FLAG"):
                r.setdefault(col, 0.0)


# ─── Compute replay labels ─────────────────────────────────────────────────────

def _label_rows(rows: List[dict]) -> List[dict]:
    """
    Add replay classification labels to each row.
    Reads forward returns from RET_* / MAX_RET_* columns (computed by
    _compute_forward_returns before this call) and canonical score columns
    (FINAL_BULL_SCORE, HARD_BEAR_SCORE, ROCKET_SCORE, FINAL_REGIME) now
    present in stock_stat CSV since canonical_scoring_engine upgrade.
    """
    for r in rows:
        # Forward returns — computed from OHLCV by _compute_forward_returns()
        ret1  = _f(r.get("RET_1D",  0))
        ret3  = _f(r.get("RET_3D",  0))
        ret5  = _f(r.get("RET_5D",  0))
        ret10 = _f(r.get("RET_10D", 0))
        max5  = _f(r.get("MAX_RET_5D",  0))
        max10 = _f(r.get("MAX_RET_10D", 0))

        # Canonical score columns (now in stock_stat CSV via canonical engine)
        fbs   = _bull_score(r)                                     # FINAL_BULL_SCORE / turbo_score
        hbs   = _n(r, "HARD_BEAR_SCORE", "BEARISH_RISK_SCORE")
        rocket= _n(r, "ROCKET_SCORE")

        r["_ret1"]  = ret1
        r["_ret3"]  = ret3
        r["_ret5"]  = ret5
        r["_ret10"] = ret10
        r["_max5"]  = max5
        r["_max10"] = max10

        r["_bw3"]   = max5  >= 8.0
        r["_bw5"]   = max5  >= 12.0
        r["_bw10"]  = max10 >= 20.0
        r["_para"]  = max10 >= 30.0
        r["_cw5"]   = ret5  >= 5.0
        r["_cw10"]  = ret10 >= 8.0
        r["_fail5"] = ret5  <= -8.0
        r["_fail10"]= ret10 <= -12.0 or max5 <= -12.0
        r["_ext"]   = _bool(r, "ALREADY_EXTENDED_FLAG")

        # Missed winner: big 10D win that the system scored below threshold
        r["_missed"]  = r["_bw10"] and fbs < 60 and hbs < 40
        r["_fp"]      = fbs >= 100 and r["_fail10"]
        r["_ls_win"]  = fbs < 40  and r["_bw10"]
        r["_hs_win"]  = fbs >= 100 and r["_bw10"]
        r["_hs_fail"] = fbs >= 100 and r["_fail10"]
        r["_rkt_miss"]= r["_para"] and rocket < 25
        r["_bear_win"]= _str(r, "FINAL_REGIME") == "BEARISH_PHASE" and r["_bw10"]
        r["_missed_cat"] = ""  # filled by _classify_missed_winners()

    return rows


# ─── Aggregation helper ───────────────────────────────────────────────────────

def _agg(rows: List[dict]) -> dict:
    n = len(rows)
    if not n:
        return {"count": 0}
    return {
        "count":                n,
        "avg_ret_1d":           _mean([r["_ret1"]  for r in rows]),
        "avg_ret_3d":           _mean([r["_ret3"]  for r in rows]),
        "avg_ret_5d":           _mean([r["_ret5"]  for r in rows]),
        "avg_ret_10d":          _mean([r["_ret10"] for r in rows]),
        "median_ret_10d":       _median([r["_ret10"] for r in rows]),
        "avg_max_high_5d":      _mean([r["_max5"]  for r in rows]),
        "avg_max_high_10d":     _mean([r["_max10"] for r in rows]),
        "big_win_10d_rate":     _rate([r["_bw10"]  for r in rows]),
        "parabolic_10d_rate":   _rate([r["_para"]  for r in rows]),
        "fail_10d_rate":        _rate([r["_fail10"] for r in rows]),
        "clean_win_5d_rate":    _rate([r["_cw5"]   for r in rows]),
    }


# ─── Section 5: Score bucket performance ──────────────────────────────────────

# All v4.4 score components — order matters for display
_SCORE_COLS = [
    ("FINAL_BULL_SCORE",        "FINAL_BULL"),
    ("GOG_SCORE",               "GOG"),
    ("SIGNAL_SCORE",            "SIGNAL"),
    ("turbo_score",             "TURBO"),
    ("CLEAN_ENTRY_SCORE",       "CLEAN_ENTRY"),
    ("SHAKEOUT_ABSORB_SCORE",   "SHAKEOUT_ABSORB"),
    ("ROCKET_SCORE",            "ROCKET"),
    ("EXTRA_BULL_SCORE",        "EXTRA_BULL"),
    ("EXPERIMENTAL_SCORE",      "EXPERIMENTAL"),
    ("REBOUND_SQUEEZE_SCORE",   "REBOUND_SQUEEZE"),
    ("HARD_BEAR_SCORE",         "HARD_BEAR"),
    ("VOLATILITY_RISK_SCORE",   "VOLATILITY_RISK"),
    ("BEARISH_RISK_SCORE",      "BEARISH_RISK"),   # backward-compat alias
    ("rtb_total",               "RTB"),
]

_BUCKET_EDGES = [(0,20,"<20"),(20,40,"20-39"),(40,60,"40-59"),(60,80,"60-79"),
                  (80,100,"80-99"),(100,120,"100-119"),(120,140,"120-139"),(140,9999,"140+")]

def score_bucket_perf(rows: List[dict]) -> List[dict]:
    out = []
    seen = set()
    for csv_col, label in _SCORE_COLS:
        if label in seen:
            continue
        if not any(csv_col in r and _f(r.get(csv_col, 0)) > 0 for r in rows[:50]):
            continue
        seen.add(label)
        for lo, hi, bucket in _BUCKET_EDGES:
            bucket_rows = [r for r in rows if lo <= _f(r.get(csv_col, 0)) < hi]
            if not bucket_rows:
                continue
            d = _agg(bucket_rows)
            d["score_name"] = label
            d["bucket"] = bucket
            out.append(d)
    return out


# ─── Section 6: Regime performance ────────────────────────────────────────────

def regime_perf(rows: List[dict]) -> List[dict]:
    groups: Dict[str, list] = {}
    for r in rows:
        reg = _str(r, "FINAL_REGIME") or "NONE"
        groups.setdefault(reg, []).append(r)
    out = []
    for reg, grp in sorted(groups.items(), key=lambda x: -len(x[1])):
        d = _agg(grp)
        d["final_regime"] = reg
        tops = sorted(grp, key=lambda r: -r["_max10"])[:5]
        d["top_examples"] = "|".join(
            f"{_str(r,'ticker')}@{_str(r,'date')}(+{r['_max10']:.1f}%)" for r in tops)
        out.append(d)
    return out


# ─── Section 9: Signal performance ────────────────────────────────────────────

def signal_perf(rows: List[dict], min_count: int = 20) -> List[dict]:
    active = _nonzero_cols(rows, _ALL_SIG_COLS)
    total  = len(rows)
    out = []
    for sig in active:
        sr = [r for r in rows if _f(r.get(sig, 0)) > 0]
        n  = len(sr)
        if n < min_count:
            continue
        d = _agg(sr)
        d["signal"] = sig
        d["frequency"] = round(n / total, 4) if total else 0
        d["is_scored"] = int(sig in _SCORED)
        out.append(d)
    out.sort(key=lambda x: -(x.get("avg_ret_10d") or 0))
    return out


# ─── Section 10: Pair / triple combos ─────────────────────────────────────────

def _freq_cols(rows: List[dict], min_n: int, cap: int) -> List[str]:
    active = _nonzero_cols(rows, _ALL_SIG_COLS)
    freq = [(c, sum(1 for r in rows if _f(r.get(c, 0)) > 0)) for c in active]
    freq.sort(key=lambda x: -x[1])
    return [c for c, n in freq if n >= min_n][:cap]

def pair_combo_perf(rows: List[dict], min_count: int = 30, top_n: int = 100) -> List[dict]:
    cols = _freq_cols(rows, min_count, 60)
    stats: Dict[tuple, list] = {}
    for r in rows:
        active = [c for c in cols if _f(r.get(c, 0)) > 0]
        for a, b in itertools.combinations(active, 2):
            key = (a, b) if a < b else (b, a)
            stats.setdefault(key, []).append(r)
    out = []
    for key, pr in stats.items():
        if len(pr) < min_count:
            continue
        d = _agg(pr)
        d["combo"] = f"{key[0]}+{key[1]}"
        out.append(d)
    out.sort(key=lambda x: -(x.get("avg_ret_10d") or 0))
    return out[:top_n]

def triple_combo_perf(rows: List[dict], min_count: int = 20, top_n: int = 100) -> List[dict]:
    cols = _freq_cols(rows, min_count * 2, 40)
    stats: Dict[tuple, list] = {}
    for r in rows:
        active = [c for c in cols if _f(r.get(c, 0)) > 0]
        for combo in itertools.combinations(active, 3):
            key = tuple(sorted(combo))
            stats.setdefault(key, []).append(r)
    out = []
    for key, pr in stats.items():
        if len(pr) < min_count:
            continue
        d = _agg(pr)
        d["combo"] = "+".join(key)
        out.append(d)
    out.sort(key=lambda x: -(x.get("avg_ret_10d") or 0))
    return out[:top_n]


# ─── Section 11: Named model performance ──────────────────────────────────────

def model_perf(rows: List[dict]) -> List[dict]:
    out = []
    for col in _MODEL_COLS:
        mr = [r for r in rows if _f(r.get(col, 0)) > 0]
        if not mr:
            continue
        d = _agg(mr)
        d["model"] = col
        out.append(d)
    out.sort(key=lambda x: -(x.get("avg_ret_10d") or 0))
    return out


# ─── Section 12: Profile playbook performance ─────────────────────────────────

def profile_perf(rows: List[dict]) -> List[dict]:
    """Group rows by profile_name × profile_category; compute standard perf metrics."""
    groups: Dict[str, list] = {}
    for r in rows:
        pname = r.get("profile_name") or "UNKNOWN"
        pcat  = r.get("profile_category") or "WATCH"
        key   = f"{pname}|{pcat}"
        groups.setdefault(key, []).append(r)
    out = []
    for key, grp in sorted(groups.items(), key=lambda x: -x[1][0].get("profile_score", 0) if x[1] else 0):
        pname, pcat = key.split("|", 1)
        d = _agg(grp)
        d["profile_name"]     = pname
        d["profile_category"] = pcat
        d["avg_pf_score"]     = round(_mean([float(r.get("profile_score", 0)) for r in grp]), 1)
        tops = sorted(grp, key=lambda r: -r["_max10"])[:3]
        d["top_examples"] = "|".join(
            f"{_str(r,'ticker')}@{_str(r,'date')}(+{r['_max10']:.1f}%)" for r in tops)
        out.append(d)
    out.sort(key=lambda x: (x["profile_name"], x["profile_category"]))
    return out


def sweet_spot_perf(rows: List[dict]) -> List[dict]:
    """Compare sweet_spot_active=True vs False per profile; key validation report."""
    by_profile: Dict[str, Dict[str, list]] = {}
    for r in rows:
        pname = r.get("profile_name") or "UNKNOWN"
        is_ss = bool(r.get("sweet_spot_active"))
        by_profile.setdefault(pname, {}).setdefault("sweet" if is_ss else "other", []).append(r)
    out = []
    for pname, buckets in sorted(by_profile.items()):
        for label, grp in buckets.items():
            if not grp:
                continue
            d = _agg(grp)
            d["profile_name"] = pname
            d["bucket"]       = label   # "sweet" | "other"
            d["avg_pf_score"] = round(_mean([float(r.get("profile_score", 0)) for r in grp]), 1)
            out.append(d)
    return out


# ─── Bear-to-bull sequence analytics ─────────────────────────────────────────

def bear_to_bull_perf(rows: List[dict]) -> List[dict]:
    """Group rows by bear_signal × bull_signal × bars_ago × profile_name × profile_category.

    Only includes rows where bear_to_bull_confirmed=1 and bear_to_bull_pairs is populated.
    """
    groups: Dict[str, list] = {}
    for r in rows:
        if not r.get("bear_to_bull_confirmed"):
            continue
        pairs_raw = r.get("bear_to_bull_pairs", "")
        if isinstance(pairs_raw, list):
            pairs = pairs_raw
        else:
            pairs = [p.strip() for p in str(pairs_raw).split() if p.strip()]
        pname = r.get("profile_name") or "UNKNOWN"
        pcat  = r.get("profile_category") or "WATCH"
        for pair_str in pairs:
            # format: BEAR->BULL@N
            m = pair_str.split("@")
            if len(m) != 2:
                continue
            signals_part, bars_ago_str = m
            parts = signals_part.split("->")
            if len(parts) != 2:
                continue
            bear_sig, bull_sig = parts
            try:
                bars_ago = int(bars_ago_str)
            except ValueError:
                continue
            key = f"{bear_sig}|{bull_sig}|{bars_ago}|{pname}|{pcat}"
            groups.setdefault(key, []).append(r)

    out = []
    for key, grp in groups.items():
        parts = key.split("|")
        bear_sig, bull_sig, bars_ago, pname, pcat = parts
        d = _agg(grp)
        d["bear_signal"]      = bear_sig
        d["bull_signal"]      = bull_sig
        d["bars_ago"]         = int(bars_ago)
        d["profile_name"]     = pname
        d["profile_category"] = pcat
        out.append(d)
    out.sort(key=lambda x: -x.get("count", 0))
    return out


def bear_to_bull_summary(rows: List[dict]) -> List[dict]:
    """Group rows by bear->bull pair × bars_ago_bucket × profile_name.

    bars_ago_bucket: 1, 2-3, 4-5
    """
    groups: Dict[str, list] = {}
    for r in rows:
        if not r.get("bear_to_bull_confirmed"):
            continue
        pairs_raw = r.get("bear_to_bull_pairs", "")
        if isinstance(pairs_raw, list):
            pairs = pairs_raw
        else:
            pairs = [p.strip() for p in str(pairs_raw).split() if p.strip()]
        pname = r.get("profile_name") or "UNKNOWN"
        for pair_str in pairs:
            m = pair_str.split("@")
            if len(m) != 2:
                continue
            signals_part, bars_ago_str = m
            parts = signals_part.split("->")
            if len(parts) != 2:
                continue
            bear_sig, bull_sig = parts
            try:
                bars_ago = int(bars_ago_str)
            except ValueError:
                continue
            if bars_ago == 1:
                bucket = "1"
            elif bars_ago <= 3:
                bucket = "2-3"
            else:
                bucket = "4-5"
            pair_label = f"{bear_sig}->{bull_sig}"
            key = f"{pair_label}|{bucket}|{pname}"
            groups.setdefault(key, []).append(r)

    out = []
    for key, grp in groups.items():
        parts = key.split("|")
        pair_label, bucket, pname = parts
        d = _agg(grp)
        d["pair"]         = pair_label
        d["bars_ago_bucket"] = bucket
        d["profile_name"] = pname
        out.append(d)
    out.sort(key=lambda x: (-x.get("count", 0), x.get("pair", "")))
    return out


def btb_category_upgrade_perf(rows: List[dict]) -> List[dict]:
    """Performance of rows where BTB caused a category upgrade.

    Groups by: category_without_btb × category_with_btb × profile_name.
    Useful for measuring whether BTB-created SWEET_SPOT rows actually perform.
    """
    groups: Dict[str, list] = {}
    for r in rows:
        if not r.get("btb_category_upgrade"):
            continue
        cat_before = str(r.get("category_without_btb", "WATCH"))
        cat_after  = str(r.get("category_with_btb",    "WATCH"))
        pname      = str(r.get("profile_name", "UNKNOWN"))
        created_ss = int(r.get("btb_created_sweet_spot", 0))
        key = f"{cat_before}|{cat_after}|{pname}|{created_ss}"
        groups.setdefault(key, []).append(r)

    out = []
    for key, grp in groups.items():
        cat_before, cat_after, pname, created_ss = key.split("|")
        d = _agg(grp)
        d["category_without_btb"]  = cat_before
        d["category_with_btb"]     = cat_after
        d["profile_name"]          = pname
        d["btb_created_sweet_spot"] = int(created_ss)
        out.append(d)
    out.sort(key=lambda x: (-x.get("count", 0), x.get("category_with_btb", "")))
    return out


def btb_pair_calibration(rows: List[dict]) -> List[dict]:
    """Per-pair calibration: performance by bear×bull pair × bars_ago bucket.

    Separates BTB-created-SWEET_SPOT rows from non-upgraded rows for each pair.
    Enables per-pair quality assessment.
    """
    groups: Dict[str, list] = {}
    for r in rows:
        if not r.get("bear_to_bull_confirmed"):
            continue
        pairs_raw = r.get("bear_to_bull_pairs", "")
        if isinstance(pairs_raw, list):
            pairs = pairs_raw
        else:
            pairs = [p.strip() for p in str(pairs_raw).split() if p.strip()]
        created_ss = int(r.get("btb_created_sweet_spot", 0))
        for pair_str in pairs:
            m = pair_str.split("@")
            if len(m) != 2:
                continue
            signals_part, bars_ago_str = m
            parts = signals_part.split("->")
            if len(parts) != 2:
                continue
            bear_sig, bull_sig = parts
            try:
                bars_ago = int(bars_ago_str)
            except ValueError:
                continue
            if bars_ago == 1:
                bucket = "1"
            elif bars_ago <= 3:
                bucket = "2-3"
            else:
                bucket = "4-5"
            key = f"{bear_sig}|{bull_sig}|{bucket}|{created_ss}"
            groups.setdefault(key, []).append(r)

    out = []
    for key, grp in groups.items():
        bear_sig, bull_sig, bucket, created_ss = key.split("|")
        d = _agg(grp)
        d["bear_signal"]           = bear_sig
        d["bull_signal"]           = bull_sig
        d["bars_ago_bucket"]       = bucket
        d["btb_created_sweet_spot"] = int(created_ss)
        out.append(d)
    out.sort(key=lambda x: (-x.get("count", 0), x.get("bear_signal", "")))
    return out


# ─── Miss reason (internal helper) ────────────────────────────────────────────

def _miss_reason(r: dict) -> str:
    fbs    = _bull_score(r)
    regime = _str(r, "FINAL_REGIME")
    rocket = _f(r.get("ROCKET_SCORE", 0))
    parts  = []
    if fbs < 20:
        parts.append("A:score_very_low")
    elif fbs < 60:
        parts.append("A:score_below_60")
    if regime in ("NEUTRAL_OR_LOW", "BEARISH_PHASE", "NONE", ""):
        parts.append("B:neutral_regime")
    if r.get("_ext"):
        parts.append("G:already_extended")
    if rocket < 15:
        parts.append("H:rocket_too_weak")
    active = _active_sigs(r)
    if not any(s in _SCORED for s in active):
        parts.append("E:all_unscored_signals")
    if not _f(r.get("HAS_ELITE_MODEL", 0)) and not _f(r.get("HAS_REBOUND_MODEL", 0)):
        parts.append("D:no_named_model")
    return "|".join(parts) if parts else "J:unknown"


def _missed_row(r: dict) -> dict:
    """
    Build a standard missed-winner output row.
    final_bull_score is read directly from the stock_stat CSV column — same value
    as per-ticker export and all other replay reports.
    """
    return {
        # ── Identity ──────────────────────────────────────────────────────────
        "ticker":                    _str(r, "ticker"),
        "date":                      _str(r, "date"),
        "close":                     _n(r, "close"),
        "missed_category":           r.get("_missed_cat", ""),
        # ── Scores (direct from stock_stat CSV — same source as per-ticker export)
        "final_bull_score":          _bull_score(r),
        "gog_score":                 _n(r, "GOG_SCORE"),
        "turbo_score":               _n(r, "turbo_score"),
        "signal_score":              _n(r, "SIGNAL_SCORE"),
        "rocket_score":              _n(r, "ROCKET_SCORE"),
        "rebound_squeeze_score":     _n(r, "REBOUND_SQUEEZE_SCORE"),
        "rtb_score":                 _n(r, "rtb_total"),
        # ── Context ───────────────────────────────────────────────────────────
        "final_regime":              _str(r, "FINAL_REGIME"),
        "final_score_bucket":        _str(r, "FINAL_SCORE_BUCKET"),
        "already_extended":          int(r["_ext"]),
        # ── Prior-catch fields (lookback classification evidence) ─────────────
        "prior_actionable_found":    int(r.get("_prior_actionable", False)),
        "best_prior_score_3d":       r.get("_best_prior_score_3d",  0.0),
        "best_prior_score_5d":       r.get("_best_prior_score_5d",  0.0),
        "best_prior_score_10d":      r.get("_best_prior_score_10d", 0.0),
        "best_prior_score_20d":      r.get("_best_prior_score_20d", 0.0),
        "best_prior_date_20d":       r.get("_best_prior_date_20d",  ""),
        "best_prior_regime_20d":     r.get("_best_prior_regime_20d",""),
        "days_since_best_prior_catch": r.get("_days_since_best_prior", 0),
        # ── GOG/VBO event timing (informational only — not used for classification)
        "gog_w5":                    int(r.get("_gog_w5",  0)),
        "gog_w10":                   int(r.get("_gog_w10", 0)),
        "vbo_w5":                    int(r.get("_vbo_w5",  0)),
        "vbo_w10":                   int(r.get("_vbo_w10", 0)),
        "bars_to_gog":               r.get("_bars_to_gog", 0),
        "bars_to_vbo":               r.get("_bars_to_vbo", 0),
        "ret_to_gog_high":           r.get("_ret_to_gog",  0.0),
        "ret_to_vbo_high":           r.get("_ret_to_vbo",  0.0),
        # ── Outcomes ─────────────────────────────────────────────────────────
        "ret_3d":                    r["_ret3"],
        "ret_5d":                    r["_ret5"],
        "ret_10d":                   r["_ret10"],
        "max_high_5d":               r["_max5"],
        "max_high_10d":              r["_max10"],
        "active_signals":            "|".join(_active_sigs(r)),
        "likely_miss_reason":        _miss_reason(r),
    }


# ─── Section 7: All missed big winners (unified) ──────────────────────────────

def missed_winners(rows: List[dict], top_n: Optional[int] = 500) -> List[dict]:
    missed = sorted([r for r in rows if r["_missed"]], key=lambda r: -r["_max10"])
    if top_n is not None:
        missed = missed[:top_n]
    return [_missed_row(r) for r in missed]


# ─── Section 7a: TRUE_MISSED_WINNERS ──────────────────────────────────────────

def true_missed_winners(rows: List[dict], top_n: Optional[int] = 300) -> List[dict]:
    """
    BIG_WIN_10D=True, FINAL_BULL_SCORE < 60, not extended, AND no prior actionable
    score/regime/model in the 20 trading days before this date for the same ticker.
    These are genuine scoring formula gaps. Pass top_n=None for full export.
    """
    subset = sorted(
        [r for r in rows if r.get("_missed_cat") == "TRUE_MISSED_WINNER"],
        key=lambda r: -r["_max10"],
    )
    if top_n is not None:
        subset = subset[:top_n]
    return [_missed_row(r) for r in subset]


# ─── Section 7b: CAUGHT_EARLY_WINNERS ─────────────────────────────────────────

def caught_early_winners(rows: List[dict], top_n: Optional[int] = 300) -> List[dict]:
    """
    BIG_WIN_10D=True, FINAL_BULL_SCORE<60, not extended, AND at least one prior row
    within 20 sessions for the same ticker was actionable (score>=60, bullish regime,
    or named model). The system surfaced the ticker earlier; this is a follow-through entry.
    Pass top_n=None for full export.
    """
    subset = sorted(
        [r for r in rows if r.get("_missed_cat") == "CAUGHT_EARLY_WINNER"],
        key=lambda r: -r["_max10"],
    )
    if top_n is not None:
        subset = subset[:top_n]
    return [_missed_row(r) for r in subset]


# ─── Section 7c: LATE_OR_WEAK_CATCHES ─────────────────────────────────────────

def late_or_weak_catches(rows: List[dict], top_n: Optional[int] = 300) -> List[dict]:
    """
    BIG_WIN_10D=True, FINAL_BULL_SCORE<60, AND already_extended=True at signal time.
    Entry would have been chasing price — not a scoring formula failure.
    Pass top_n=None for full export.
    """
    subset = sorted(
        [r for r in rows if r.get("_missed_cat") == "LATE_OR_WEAK_CATCH"],
        key=lambda r: -r["_max10"],
    )
    if top_n is not None:
        subset = subset[:top_n]
    return [_missed_row(r) for r in subset]


# ─── Section 8: False positives ───────────────────────────────────────────────

def _fail_reason(r: dict) -> str:
    hbs    = _f(r.get("HARD_BEAR_SCORE", r.get("BEARISH_RISK_SCORE", 0)))
    parts  = []
    if r.get("_ext"):
        parts.append("E:extended_top")
    if hbs > 20:
        parts.append("C:vol_or_bear_risk_ignored")
    active = _active_sigs(r)
    bear   = [s for s in active if "DN" in s or s in ("SIG_BC","SIG_BIAS_DN","SIG_RH")]
    if bear:
        parts.append(f"D:bearish_signal:{bear[0]}")
    if not any(_f(r.get(m, 0)) for m in _MODEL_COLS):
        parts.append("A:weak_signal_stacking")
    return "|".join(parts) if parts else "A:weak_signal_stacking"

def false_positives(rows: List[dict], top_n: int = 500) -> List[dict]:
    fps = sorted([r for r in rows if r["_fp"]], key=lambda r: r["_ret10"])
    out = []
    for r in fps[:top_n]:
        out.append({
            "ticker":             _str(r, "ticker"),
            "date":               _str(r, "date"),
            "close":              _n(r, "close"),
            "final_bull_score":   _bull_score(r),
            "final_regime":       _str(r, "FINAL_REGIME"),
            "final_score_bucket": _str(r, "FINAL_SCORE_BUCKET"),
            "hard_bear_score":    _n(r, "HARD_BEAR_SCORE", "BEARISH_RISK_SCORE"),
            "volatility_risk_score": _n(r, "VOLATILITY_RISK_SCORE"),
            "already_extended":   int(r["_ext"]),
            "ret_5d":             r["_ret5"],
            "ret_10d":            r["_ret10"],
            "max_high_10d":       r["_max10"],
            "active_signals":     "|".join(_active_sigs(r)),
            "active_models":      "|".join(m for m in _MODEL_COLS if _f(r.get(m, 0))),
            "likely_fail_reason": _fail_reason(r),
        })
    return out


# ─── Section 12: Active but unscored signals ──────────────────────────────────

def unscored_signals(rows: List[dict], min_count: int = 20) -> List[dict]:
    active = _nonzero_cols(rows, _ALL_SIG_COLS)
    unscored = [c for c in active if c not in _SCORED]
    out = []
    for sig in unscored:
        sr = [r for r in rows if _f(r.get(sig, 0)) > 0]
        n  = len(sr)
        if n < min_count:
            continue
        avg_ret  = _mean([r["_ret10"] for r in sr])
        bw_rate  = _rate([r["_bw10"]  for r in sr])
        fail_rate= _rate([r["_fail10"]for r in sr])
        if avg_ret and avg_ret > 3 and bw_rate and bw_rate > 0.08:
            suggested = "ROCKET_SCORE" if any(x in sig for x in ("ROCKET","PARA","FLY","VOL")) else "CLEAN_ENTRY_SCORE"
        elif avg_ret and avg_ret < -2 and fail_rate and fail_rate > 0.15:
            suggested = "HARD_BEAR_SCORE"
        elif avg_ret and avg_ret > 1:
            suggested = "EXTRA_BULL_SCORE"
        else:
            suggested = "monitor_only"
        out.append({
            "signal":             sig,
            "count":              n,
            "avg_ret_10d":        avg_ret,
            "big_win_10d_rate":   bw_rate,
            "fail_10d_rate":      fail_rate,
            "suggested_component": suggested,
            "suggested_action":   "add_to_score" if (avg_ret or 0) > 2 else "monitor",
        })
    out.sort(key=lambda x: -(x.get("avg_ret_10d") or 0))
    return out


# ─── Section 13: Scored but weak ──────────────────────────────────────────────

def scored_weak(rows: List[dict], min_count: int = 20) -> List[dict]:
    out = []
    for sig, (component, weight) in _SIG_WEIGHTS.items():
        sr = [r for r in rows if _f(r.get(sig, 0)) > 0]
        n  = len(sr)
        if n < min_count:
            continue
        avg_ret  = _mean([r["_ret10"] for r in sr])
        bw_rate  = _rate([r["_bw10"]  for r in sr])
        fail_rate= _rate([r["_fail10"]for r in sr])
        is_bear = component in ("HARD_BEAR_SCORE", "VOLATILITY_RISK_SCORE")
        if is_bear:
            rec = "keep_bearish" if (avg_ret or 0) < 0 else "review_context"
        elif (avg_ret or 0) < 0 and not is_bear:
            rec = "reduce_weight"
        elif (fail_rate or 0) > 0.25:
            rec = "require_combo_confirmation"
        elif (bw_rate or 0) > 0.15:
            rec = "keep"
        elif 0 <= (avg_ret or 0) < 1:
            rec = "move_to_helper_score"
        else:
            rec = "keep"
        out.append({
            "signal":            sig,
            "current_component": component,
            "current_weight":    weight,
            "count":             n,
            "avg_ret_10d":       avg_ret,
            "big_win_10d_rate":  bw_rate,
            "fail_10d_rate":     fail_rate,
            "recommendation":    rec,
        })
    out.sort(key=lambda x: x.get("avg_ret_10d") or 0)
    return out


# ─── Section 14: Filter miss audit ────────────────────────────────────────────

def filter_miss_audit(rows: List[dict]) -> List[dict]:
    filters = [
        ("ALREADY_EXTENDED_FLAG", lambda r: r["_ext"]),
        ("FINAL_BULL_SCORE < 40",  lambda r: _bull_score(r) < 40),
        ("FINAL_BULL_SCORE < 60",  lambda r: _bull_score(r) < 60),
        ("BEARISH_PHASE_REGIME",   lambda r: _str(r,"FINAL_REGIME") == "BEARISH_PHASE"),
        ("NO_ELITE_MODEL",         lambda r: not _f(r.get("HAS_ELITE_MODEL",0))),
        ("HARD_BEAR >= 40",        lambda r: _f(r.get("HARD_BEAR_SCORE",r.get("BEARISH_RISK_SCORE",0))) >= 40),
        ("NEUTRAL_OR_LOW_REGIME",  lambda r: _str(r,"FINAL_REGIME") in ("NEUTRAL_OR_LOW","","NONE")),
        ("REBOUND_SQUEEZE < 10",   lambda r: _f(r.get("REBOUND_SQUEEZE_SCORE",0)) < 10),
        ("NO_REBOUND_MODEL",       lambda r: not _f(r.get("HAS_REBOUND_MODEL",0))),
    ]
    out = []
    for name, fn in filters:
        excl = [r for r in rows if fn(r)]
        n    = len(excl)
        if not n:
            continue
        missed = [r for r in excl if r["_bw10"]]
        tops   = sorted(missed, key=lambda r: -r["_max10"])[:3]
        out.append({
            "filter_name":                 name,
            "excluded_count":              n,
            "missed_big_win_count":        len(missed),
            "missed_big_win_rate":         round(len(missed)/n, 4) if n else 0,
            "avg_max_high_10d_excluded":   _mean([r["_max10"] for r in excl]),
            "avg_max_high_10d_missed":     _mean([r["_max10"] for r in missed]),
            "top_missed_examples":         "|".join(
                f"{_str(r,'ticker')}@{_str(r,'date')}(+{r['_max10']:.1f}%)" for r in tops),
        })
    return out


# ─── Section 4: Split analytics ───────────────────────────────────────────────

def split_analytics(rows: List[dict]) -> dict:
    return {
        "available": False,
        "message": "Split analytics disabled.",
        "events": [], "missed": [], "false_positives": [],
    }


# ─── Markdown summary ─────────────────────────────────────────────────────────

def _md_summary(reports: dict, gen_at: str, tf: str, universe: str, n: int) -> str:
    from canonical_scoring_engine import SCORING_ENGINE_NAME, SCORING_ENGINE_VERSION
    meta = reports.get("scoring_metadata", {})
    score_eng = meta.get("scoring_engine_name", SCORING_ENGINE_NAME)
    score_ver = meta.get("scoring_engine_version", SCORING_ENGINE_VERSION)
    bars_used = meta.get("bars_used", "unknown")
    score_consistency = reports.get("score_consistency", {})
    sc_status = score_consistency.get("status", "not_run")
    sc_mismatches = score_consistency.get("mismatch_count", 0)

    lines = [
        "# Replay Analytics Summary",
        "",
        f"Generated: {gen_at}  ",
        f"Dataset: **{universe}** / **{tf}** — {n:,} rows  ",
        f"Scoring engine: **{score_eng}** v{score_ver}  ",
        f"Canonical score source: **true**  ",
        f"Bars used in stock_stat: **{bars_used}**",
        "",
        "---",
        "",
        "## 0. Score Consistency",
        "",
    ]
    if sc_status == "ok":
        lines.append("✓ Score consistency: **PASS** — all sampled scores match canonical engine.")
    elif sc_status == "not_run":
        lines.append("⚠ Score consistency check: **not run** (no reference samples configured).")
    else:
        lines += [
            f"✗ Score consistency: **FAIL** — {sc_mismatches} mismatch(es) detected.",
            "",
            "**Replay score consistency validation FAILED. Reports should not be used.**",
            "",
            "Re-run Stock Stat scan to regenerate data with the canonical scoring engine,",
            "then re-run Replay.",
        ]
    lines += ["", "---", "", "## 1. Executive Summary", ""]

    # Score monotonicity
    sbp = reports.get("score_bucket_perf", [])
    fbs_rows = [r for r in sbp if r.get("score_name") == "FINAL_BULL"]
    if fbs_rows:
        by_bucket = {r["bucket"]: r for r in fbs_rows}
        elite = by_bucket.get("140+", {})
        low   = by_bucket.get("<20", {})
        lines.append(f"- FINAL_BULL_SCORE 140+ avg 10D return: **{elite.get('avg_ret_10d')}** "
                     f"vs <20 bucket: **{low.get('avg_ret_10d')}**")
        lines.append(f"- FINAL_BULL_SCORE 140+ BIG_WIN_10D rate: **{elite.get('big_win_10d_rate')}**")

    # Missed winner category breakdown — counts derived from FULL category files
    # so summary and exported files cannot disagree.
    mw_all  = reports.get("missed_winners_full", []) or reports.get("missed_winners", [])
    counts  = reports.get("category_counts", {})
    total_m = counts.get("total_missed") or len(mw_all)
    if total_m:
        true_full   = reports.get("true_missed_winners_full",   [])
        caught_full = reports.get("caught_early_winners_full",  [])
        late_full   = reports.get("late_or_weak_catches_full",  [])
        true_top    = reports.get("true_missed_winners_top300", [])
        caught_top  = reports.get("caught_early_winners_top300",[])
        late_top    = reports.get("late_or_weak_catches_top300",[])

        true_n   = len(true_full)
        caught_n = len(caught_full)
        late_n   = len(late_full)
        cat_sum  = true_n + caught_n + late_n

        # Cross-check actual counts vs full files vs counts dict
        match_counts = (
            true_n   == counts.get("true_missed",  -1) and
            caught_n == counts.get("caught_early", -1) and
            late_n   == counts.get("late_or_weak", -1)
        )
        sum_check = "✓ PASS" if (cat_sum == total_m and match_counts) else (
            f"✗ FAIL (full_sum={cat_sum}, total={total_m}, "
            f"true={true_n}/{counts.get('true_missed')}, "
            f"caught={caught_n}/{counts.get('caught_early')}, "
            f"late={late_n}/{counts.get('late_or_weak')})"
        )

        lines += ["", "## 2. Missed Winner Breakdown", ""]
        lines.append(f"Total missed big winners (max_high_10d ≥ 20%, FINAL_BULL_SCORE < 60): **{total_m}**")
        lines.append(f"Category sum check: {sum_check}")
        lines.append("")
        lines.append("| Category | Actual Count | Exported (full) | Exported (top300) | % of Missed | Avg Max High 10D |")
        lines.append("|----------|-------------|-----------------|-------------------|-------------|-----------------|")
        for label, full_rows, top_rows in [
            ("TRUE_MISSED_WINNERS",  true_full,   true_top),
            ("CAUGHT_EARLY_WINNERS", caught_full, caught_top),
            ("LATE_OR_WEAK_CATCHES", late_full,   late_top),
        ]:
            actual_n = len(full_rows)
            top_n    = len(top_rows)
            pct      = round(actual_n / total_m * 100, 1) if total_m else 0
            avg_mh   = _mean([r.get("max_high_10d", 0) for r in full_rows]) if full_rows else None
            lines.append(f"| {label} | {actual_n} | {actual_n} | {top_n} | {pct}% | {avg_mh} |")
        lines.append("")
        lines.append("- **TRUE_MISSED_WINNERS** — no prior actionable score/regime/model in 20 sessions → scoring formula gap")
        lines.append("- **CAUGHT_EARLY_WINNERS** — prior actionable entry existed within 20 sessions → was catchable earlier")
        lines.append("- **LATE_OR_WEAK_CATCHES** — already extended at signal time → not a scoring failure")
        lines.append("")
        lines.append("Files: `replay_<category>_full.csv` (all rows), `replay_<category>_top300.csv` (capped).")

    # Regime table
    rp = reports.get("regime_perf", [])
    if rp:
        lines += ["", "## 6. Which regime works best?", ""]
        lines.append("| Regime | Count | Avg RET 10D | BIG_WIN % | FAIL % |")
        lines.append("|--------|-------|-------------|-----------|--------|")
        for r in sorted(rp, key=lambda x: -(x.get("avg_ret_10d") or 0))[:10]:
            lines.append(f"| {r['final_regime']} | {r['count']} | {r.get('avg_ret_10d')} | "
                         f"{r.get('big_win_10d_rate')} | {r.get('fail_10d_rate')} |")

    # Top signals
    sp = reports.get("signal_perf", [])
    if sp:
        lines += ["", "## 8. Top 20 signals by avg RET_10D", ""]
        for s in sp[:20]:
            lines.append(f"- {s['signal']} (n={s['count']}, ret={s.get('avg_ret_10d')}, "
                         f"bw={s.get('big_win_10d_rate')}, fail={s.get('fail_10d_rate')})")

    # Missed reasons (from all missed winners)
    if mw_all:
        from collections import Counter
        reason_counts: Counter = Counter()
        for r in mw_all:
            for part in r.get("likely_miss_reason","").split("|"):
                reason_counts[part.split(":")[0]] += 1
        lines += ["", "## 13. Biggest missed opportunity causes", ""]
        for reason, cnt in reason_counts.most_common(10):
            lines.append(f"- {reason}: **{cnt}** cases")

    # Split
    sa = reports.get("split_analytics", {})
    lines += ["", "## 14. Split Breakout Summary", ""]
    if sa.get("available"):
        evts = sa.get("events", [])
        fwd  = [e for e in evts if e["split_type"] == "FORWARD_SPLIT"]
        rev  = [e for e in evts if e["split_type"] == "REVERSE_SPLIT"]
        lines.append(sa.get("message",""))
        if fwd:
            lines.append(f"- Forward split avg 10D return: "
                         f"**{_mean([e['post_split_ret_10d'] for e in fwd])}**")
            lines.append(f"- Forward split breakout 10D rate: "
                         f"**{_rate([e['post_split_breakout_10d'] for e in fwd])}**")
        if rev:
            lines.append(f"- Reverse split avg 10D return: "
                         f"**{_mean([e['post_split_ret_10d'] for e in rev])}**")
    else:
        lines.append(sa.get("message", "Split data unavailable."))

    # FP summary
    fp = reports.get("false_positives", [])
    if fp:
        from collections import Counter
        fc: Counter = Counter()
        for r in fp:
            for part in r.get("likely_fail_reason","").split("|"):
                fc[part.split(":")[0]] += 1
        lines += ["", "## 12. Biggest false positive causes", ""]
        for reason, cnt in fc.most_common(8):
            lines.append(f"- {reason}: **{cnt}** cases")

    # TP/SL section — only included when TP/SL actually ran
    tpsl_status   = reports.get("tpsl_status", "skipped")
    tpsl_n_trades = reports.get("tpsl_n_trades", 0)
    tpsl_files    = reports.get("tpsl_files", []) or []
    if tpsl_status == "ok" and tpsl_files:
        lines += [
            "", "---", "",
            "## 15. TP/SL Path Analytics", "",
            f"TP/SL analytics generated: **{tpsl_n_trades:,} trade rows** across "
            f"{len(tpsl_files)} files.",
            "",
            "| Report | Purpose |",
            "|--------|---------|",
        ]
        descriptions = {
            "replay_tpsl_trades.csv":                    "Row-level trade simulations (all presets × entry modes)",
            "replay_tpsl_signal_perf.csv":               "TP/SL by signal with classification label",
            "replay_tpsl_model_perf.csv":                "TP/SL by named model",
            "replay_tpsl_regime_perf.csv":               "TP/SL by FINAL_REGIME",
            "replay_tpsl_score_bucket_perf.csv":         "TP/SL by FINAL_SCORE_BUCKET",
            "replay_tpsl_score_range_perf.csv":          "TP/SL by score range (<25/25-54/55-99/100+)",
            "replay_tpsl_pair_combo_perf.csv":           "TP/SL by signal pair",
            "replay_tpsl_triple_combo_perf.csv":         "TP/SL by signal triple",
            "replay_tpsl_missed_big_winners.csv":        "Missed/caught/late category × TP/SL",
            "replay_tpsl_false_positives.csv":           "FP classification: TRUE/TRADEABLE/VOLATILE",
            "replay_tpsl_caught_early_timing.csv":       "Prior-date entry timing for caught-early",
            "replay_tpsl_readiness_phase_perf.csv":      "TP/SL by READINESS_PHASE",
            "replay_tpsl_actionability_bucket_perf.csv": "TP/SL by ACTIONABILITY_SCORE bucket",
            "replay_tpsl_component_bucket_perf.csv":     "TP/SL by score-component buckets",
            "replay_tpsl_validation.csv":                "Validation results",
            "replay_tpsl_summary.md":                    "TP/SL analytics full summary",
            "replay_tpsl_implementation_audit.md":       "Implementation audit and changelog",
        }
        for fname in sorted(tpsl_files):
            lines.append(f"| {fname} | {descriptions.get(fname, '')} |")
        lines += [
            "",
            "Presets: SCALP_FAST / CLEAN_SWING / MOMENTUM_SWING / PARABOLIC / "
            "LOOSE_WATCH / STRUCTURAL_BUILD / VERY_TIGHT / WIDE_MOMENTUM  ",
            "Entry modes: SAME_DAY_CLOSE / NEXT_DAY_OPEN  ",
        ]
    elif tpsl_status == "failed":
        lines += [
            "", "---", "",
            "## 15. TP/SL Path Analytics", "",
            "TP/SL analytics **FAILED**. See `replay_tpsl_validation.csv` for the error reason.",
        ]
    # tpsl_status == "skipped" → omit the section entirely

    lines += [
        "", "---", "",
        "## 24. Recommended next steps", "",
        "1. Focus on **TRUE_MISSED_WINNERS** — these need scoring formula fixes",
        "2. Review **CAUGHT_EARLY_WINNERS** — consider holding or confirming later",
        "3. **LATE_OR_WEAK_CATCHES** can be deprioritized — mostly noise/extension",
        "4. Review **Scored But Weak** table for signals to reduce/remove",
        "5. Review **Active Unscored** table for signals to add",
        "6. Check **False Positives** for volatility/extension risk gaps",
    ]
    if tpsl_status == "ok":
        lines += [
            "7. Review **replay_tpsl_score_range_perf.csv** to validate phase-meter hypothesis",
            "8. Review **replay_tpsl_signal_perf.csv** to identify FAST_ENTRY vs WATCHLIST signals",
        ]
    lines += [
        "",
        "*Generated by Sachoki Replay Analytics Engine v4.4*",
    ]
    return "\n".join(lines)


# ─── Main run function ─────────────────────────────────────────────────────────

def run_replay(tf: str = "1d", universe: str = "sp500") -> None:
    """Run all replay analytics in background thread."""
    global _state
    gen_at = _now()
    _state.update(status="running", started_at=gen_at, completed_at=None,
                  progress=0, error=None, reports={}, tf=tf, universe=universe, row_count=0)
    os.makedirs(REPLAY_OUTPUT_DIR, exist_ok=True)

    def _save(name: str, data: list) -> list:
        path = os.path.join(REPLAY_OUTPUT_DIR, f"replay_{name}.csv")
        n    = _write_csv(path, data, gen_at)
        _state["reports"][name] = {"rows": n, "path": path, "generated_at": gen_at}
        return data

    cached: Dict[str, Any] = {}

    try:
        # 0 — Clean stale output files from prior runs (point E)
        n_cleaned = _clean_output_dir()
        log.info("Cleaned %d stale replay output files", n_cleaned)

        # 1 — Load
        _state["progress"] = 1; _state["message"] = "Loading stock_stat CSV..."
        rows, err = _load_stock_stat(tf, universe)
        if err:
            raise RuntimeError(err)
        _state["row_count"] = len(rows)

        # 1b — Validate scoring version (v4.4 columns present)
        score_err = _validate_score_source(rows)
        if score_err:
            raise RuntimeError(score_err)

        # 1c — Sample-cross-check stock_stat scores against per-ticker export references
        sample_err = _validate_score_samples(rows)
        if sample_err:
            raise RuntimeError(sample_err)

        # 1d — Enrich rows with profile playbook (per-ticker sequential, rolling history)
        _state["message"] = "Enriching rows with profile playbook (bear-to-bull sequence)..."
        try:
            from profile_playbook import (
                compute_profile_playbook_for_row,
                PROFILE_PLAYBOOK_VERSION as _PPV,
            )
            # Group by ticker, sort by date, process sequentially per ticker
            from collections import defaultdict
            _ticker_rows: Dict[str, list] = defaultdict(list)
            for r in rows:
                _ticker_rows[str(r.get("ticker", ""))].append(r)

            _pf_audit = {
                "rows_total": 0, "rows_with_signals": 0, "rows_pf_gt0": 0,
                "cat_dist": {}, "rows_btb": 0,
            }

            for ticker, t_rows in _ticker_rows.items():
                # Sort ascending by date
                t_rows.sort(key=lambda r: str(r.get("date", "")))
                _hist: list = []  # [most_recent_first] list of Set[str]
                for r in t_rows:
                    try:
                        pf = compute_profile_playbook_for_row(
                            r, universe, history_context=_hist[:5]
                        )
                        r["profile_playbook_version"] = pf["profile_playbook_version"]
                        r["profile_name"]             = pf["profile_name"]
                        r["profile_score"]            = pf["profile_score"]
                        r["profile_category"]         = pf["profile_category"]
                        r["sweet_spot_active"]        = int(pf["sweet_spot_active"])
                        r["late_warning"]             = int(pf["late_warning"])
                        r["bear_context_last_3"]      = pf["bear_context_last_3"]
                        r["bear_context_last_5"]      = pf["bear_context_last_5"]
                        r["bull_confirm_now"]         = pf["bull_confirm_now"]
                        r["bear_to_bull_confirmed"]          = pf["bear_to_bull_confirmed"]
                        r["bear_to_bull_bars_ago"]           = pf["bear_to_bull_bars_ago"]
                        r["bear_to_bull_bonus"]              = pf["bear_to_bull_bonus"]
                        r["bear_to_bull_pairs"]              = " ".join(pf["bear_to_bull_pairs"])
                        r["base_profile_score_without_btb"]  = pf["base_profile_score_without_btb"]
                        r["category_without_btb"]            = pf["category_without_btb"]
                        r["category_with_btb"]               = pf["category_with_btb"]
                        r["btb_category_upgrade"]            = pf["btb_category_upgrade"]
                        r["btb_created_sweet_spot"]          = pf["btb_created_sweet_spot"]
                        r["btb_late_clamped"]                = pf["btb_late_clamped"]
                        r["btb_sweet_spot_allowed_profile"]  = pf["btb_sweet_spot_allowed_profile"]
                        _hist.insert(0, set(pf["active_signals"]))
                        if len(_hist) > 5:
                            _hist.pop()
                        # Audit
                        _pf_audit["rows_total"] += 1
                        if pf["active_signals"]:
                            _pf_audit["rows_with_signals"] += 1
                        if pf["profile_score"] > 0:
                            _pf_audit["rows_pf_gt0"] += 1
                        cat = pf["profile_category"]
                        _pf_audit["cat_dist"][cat] = _pf_audit["cat_dist"].get(cat, 0) + 1
                        if pf["bear_to_bull_confirmed"]:
                            _pf_audit["rows_btb"] += 1
                    except Exception as _pf_e:
                        log.warning("profile_playbook row error ticker=%s: %s", ticker, _pf_e)
                        r.setdefault("profile_playbook_version", "")
                        r.setdefault("profile_name",     "UNKNOWN")
                        r.setdefault("profile_score",    0)
                        r.setdefault("profile_category", "WATCH")
                        r.setdefault("sweet_spot_active", 0)
                        r.setdefault("late_warning",      0)
                        r.setdefault("bear_context_last_3", 0)
                        r.setdefault("bear_context_last_5", 0)
                        r.setdefault("bull_confirm_now",    0)
                        r.setdefault("bear_to_bull_confirmed", 0)
                        r.setdefault("bear_to_bull_bars_ago",  0)
                        r.setdefault("bear_to_bull_bonus",     0)
                        r.setdefault("bear_to_bull_pairs",     "")
                        r.setdefault("base_profile_score_without_btb", 0)
                        r.setdefault("category_without_btb",   "WATCH")
                        r.setdefault("category_with_btb",      "WATCH")
                        r.setdefault("btb_category_upgrade",   0)
                        r.setdefault("btb_created_sweet_spot", 0)
                        r.setdefault("btb_late_clamped",       0)
                        r.setdefault("btb_sweet_spot_allowed_profile", 0)

            log.info(
                "PROFILE_PLAYBOOK_AUDIT replay: rows_total=%d "
                "rows_with_signals=%d rows_pf_gt0=%d cat_dist=%s rows_btb=%d",
                _pf_audit["rows_total"], _pf_audit["rows_with_signals"],
                _pf_audit["rows_pf_gt0"], _pf_audit["cat_dist"], _pf_audit["rows_btb"],
            )
            if _pf_audit["rows_with_signals"] > 0 and _pf_audit["rows_pf_gt0"] == 0:
                log.error(
                    "PROFILE_PLAYBOOK_FAILURE: %d rows have active signals "
                    "but profile_score is zero for all rows.",
                    _pf_audit["rows_with_signals"],
                )
        except ImportError:
            log.warning("profile_playbook not available — skipping profile enrichment")

        # 2 — Forward returns (computed from OHLCV in stock_stat CSV)
        _state["progress"] = 2; _state["message"] = "Computing forward returns from OHLCV..."
        _compute_forward_returns(rows)

        # 2a — Label
        _state["message"] = "Computing replay labels..."
        rows = _label_rows(rows)

        # 2b — Classify missed winners (prior-lookback; requires full labeled dataset)
        _classify_missed_winners(rows)

        # 2c — Validate categories: exhaustive + mutually exclusive
        cat_err = _validate_categories(rows)
        if cat_err:
            raise RuntimeError(cat_err)

        # 2d — Validate known classification regression cases (e.g. SNDK 2026-03-30)
        regr_err = _validate_classification_regressions(rows)
        if regr_err:
            raise RuntimeError(regr_err)

        # 2e — Store actual category counts BEFORE any top_n capping
        category_counts = {
            "total_missed":  sum(1 for r in rows if r.get("_missed")),
            "true_missed":   sum(1 for r in rows if r.get("_missed_cat") == "TRUE_MISSED_WINNER"),
            "caught_early":  sum(1 for r in rows if r.get("_missed_cat") == "CAUGHT_EARLY_WINNER"),
            "late_or_weak":  sum(1 for r in rows if r.get("_missed_cat") == "LATE_OR_WEAK_CATCH"),
        }
        cached["category_counts"] = category_counts
        log.info("Category counts: %s", category_counts)

        # 3 — Score bucket
        _state["progress"] = 3; _state["message"] = "Score bucket performance..."
        _save("score_bucket_perf", score_bucket_perf(rows))

        # 4 — Regime
        _state["progress"] = 4; _state["message"] = "Regime performance..."
        cached["regime_perf"] = _save("regime_perf", regime_perf(rows))

        # 5 — Signals
        _state["progress"] = 5; _state["message"] = "Signal performance..."
        cached["signal_perf"] = _save("signal_perf", signal_perf(rows, min_count=20))

        # 6 — Pair combos
        _state["progress"] = 6; _state["message"] = "Pair combo performance (may take a while)..."
        _save("pair_combo_perf", pair_combo_perf(rows, min_count=30, top_n=100))

        # 7 — Triple combos
        _state["progress"] = 7; _state["message"] = "Triple combo performance..."
        _save("triple_combo_perf", triple_combo_perf(rows, min_count=20, top_n=100))

        # 8 — Model perf
        _state["progress"] = 8; _state["message"] = "Named model performance..."
        _save("model_perf", model_perf(rows))

        # 8b — Profile playbook performance
        _state["message"] = "Profile playbook performance..."
        cached["profile_perf"]       = _save("profile_perf",       profile_perf(rows))
        cached["sweet_spot_perf"]    = _save("sweet_spot_perf",    sweet_spot_perf(rows))

        # 8c — Profile signal coverage audit (which tokens are unscored)
        _state["message"] = "Profile signal coverage audit..."
        try:
            from profile_playbook import profile_unscored_signals as _pus
            _save("profile_unscored_signals", _pus(rows))
        except Exception as _pus_err:
            log.warning("profile_unscored_signals failed: %s", _pus_err)

        # 8d — Bear-to-bull sequence analytics
        _state["message"] = "Bear-to-bull sequence analytics..."
        try:
            cached["bear_to_bull_perf"]    = _save("bear_to_bull_perf",    bear_to_bull_perf(rows))
            cached["bear_to_bull_summary"] = _save("bear_to_bull_summary", bear_to_bull_summary(rows))
            cached["btb_category_upgrade_perf"] = _save(
                "btb_category_upgrade_perf", btb_category_upgrade_perf(rows))
            cached["btb_pair_calibration"]      = _save(
                "btb_pair_calibration",      btb_pair_calibration(rows))
        except Exception as _btb_err:
            log.warning("bear_to_bull analytics failed: %s", _btb_err)

        # 8e — Config snapshot
        _state["message"] = "Profile playbook config snapshot..."
        try:
            from profile_playbook import get_playbook_config_snapshot
            import json as _json
            snap_path = os.path.join(REPLAY_OUTPUT_DIR, "profile_playbook_config_snapshot.json")
            with open(snap_path, "w", encoding="utf-8") as _sf:
                _json.dump(get_playbook_config_snapshot(), _sf, indent=2)
        except Exception as _snap_err:
            log.warning("Config snapshot failed: %s", _snap_err)

        # 9 — All missed winners (unified) — full + top500
        _state["progress"] = 9; _state["message"] = "Missed big winners (all categories)..."
        cached["missed_winners_full"] = _save("missed_winners_full", missed_winners(rows, top_n=None))
        cached["missed_winners"]      = _save("missed_winners",      missed_winners(rows, top_n=500))

        # 10 — TRUE_MISSED_WINNERS — full + top300
        _state["progress"] = 10; _state["message"] = "True missed winners (no prior actionable in 20 sessions)..."
        cached["true_missed_winners_full"]   = _save("true_missed_winners_full",   true_missed_winners(rows, top_n=None))
        cached["true_missed_winners_top300"] = _save("true_missed_winners_top300", true_missed_winners(rows, top_n=300))
        cached["true_missed_winners"]        = cached["true_missed_winners_top300"]  # back-compat alias

        # 11 — CAUGHT_EARLY_WINNERS — full + top300
        _state["progress"] = 11; _state["message"] = "Caught early winners (prior actionable entry existed)..."
        cached["caught_early_winners_full"]   = _save("caught_early_winners_full",   caught_early_winners(rows, top_n=None))
        cached["caught_early_winners_top300"] = _save("caught_early_winners_top300", caught_early_winners(rows, top_n=300))
        cached["caught_early_winners"]        = cached["caught_early_winners_top300"]

        # 12 — LATE_OR_WEAK_CATCHES — full + top300
        _state["progress"] = 12; _state["message"] = "Late or weak catches (already extended at signal time)..."
        cached["late_or_weak_catches_full"]   = _save("late_or_weak_catches_full",   late_or_weak_catches(rows, top_n=None))
        cached["late_or_weak_catches_top300"] = _save("late_or_weak_catches_top300", late_or_weak_catches(rows, top_n=300))
        cached["late_or_weak_catches"]        = cached["late_or_weak_catches_top300"]

        # 12b — Cross-validate: full files == actual counts; top files == min(actual, 300)
        file_check_errors = []
        for full_key, top_key, count_key in [
            ("true_missed_winners_full",  "true_missed_winners_top300",  "true_missed"),
            ("caught_early_winners_full", "caught_early_winners_top300", "caught_early"),
            ("late_or_weak_catches_full", "late_or_weak_catches_top300", "late_or_weak"),
        ]:
            actual_n  = category_counts[count_key]
            full_n    = len(cached.get(full_key, []))
            top_n_n   = len(cached.get(top_key, []))
            if full_n != actual_n:
                file_check_errors.append(
                    f"{full_key}: full file has {full_n} rows but actual category count is {actual_n}"
                )
            if top_n_n != min(actual_n, 300):
                file_check_errors.append(
                    f"{top_key}: top file has {top_n_n} rows but expected {min(actual_n, 300)}"
                )
        if file_check_errors:
            raise RuntimeError("File count mismatch: " + "; ".join(file_check_errors))

        # 13 — False positives
        _state["progress"] = 13; _state["message"] = "False positives..."
        cached["false_positives"] = _save("false_positives", false_positives(rows))

        # 14 — Unscored
        _state["progress"] = 14; _state["message"] = "Active unscored signals..."
        _save("unscored_signals", unscored_signals(rows, min_count=20))

        # 15 — Scored weak
        _state["progress"] = 15; _state["message"] = "Scored but weak signals..."
        _save("scored_weak", scored_weak(rows, min_count=20))

        # 16 — Filter audit
        _state["progress"] = 16; _state["message"] = "Filter miss audit..."
        _save("filter_miss_audit", filter_miss_audit(rows))

        # 17 — Splits
        # 17 — Split analytics: DISABLED (per project decision; module retained
        # but not invoked because it is unreliable on the NASDAQ universe).
        _state["progress"] = 17; _state["message"] = "Split analytics (disabled)..."
        cached["split_analytics"] = {
            "available": False,
            "message":   "split analytics disabled",
            "events": [], "missed": [], "false_positives": [],
        }

        # 17b — Score consistency check (live vs stock_stat + export key validation)
        _state["progress"] = 17; _state["message"] = "Score consistency check (SNDK/INTC)..."
        sc_rows, export_rows, sc_summary = _score_consistency_check(rows, tf)
        cached["score_consistency"] = sc_summary
        if sc_rows:
            sc_path = os.path.join(REPLAY_OUTPUT_DIR, "replay_score_consistency_check.csv")
            _write_csv(sc_path, sc_rows, gen_at)
            _state["reports"]["score_consistency_check"] = {
                "rows": len(sc_rows), "path": sc_path, "generated_at": gen_at
            }
        if export_rows:
            ex_path = os.path.join(REPLAY_OUTPUT_DIR, "export_score_consistency_check.csv")
            _write_csv(ex_path, export_rows, gen_at)
            _state["reports"]["export_score_consistency_check"] = {
                "rows": len(export_rows), "path": ex_path, "generated_at": gen_at
            }
        if sc_summary["status"] == "fail":
            mismatch_lines = "\n  ".join(sc_summary["mismatches"])
            log.warning(
                "Score consistency FAILED — %d mismatch(es). Replay continues; "
                "TP/SL will record the failure in tpsl_validation.csv. "
                "Mismatches: %s",
                sc_summary["mismatch_count"], mismatch_lines,
            )

        try:
            from canonical_scoring_engine import get_scoring_metadata
            sc_meta = get_scoring_metadata()
            cached["scoring_metadata"] = sc_meta
        except Exception:
            cached["scoring_metadata"] = {}

        # 18 — TP/SL path-based analytics (streaming to avoid OOM)
        # Run before summary so the summary can reflect actual TP/SL status.
        _state["progress"] = 18; _state["message"] = "TP/SL path analytics..."
        tpsl_status = "skipped"
        tpsl_n_trades = 0
        tpsl_files: List[str] = []
        try:
            from tpsl_engine import run_tpsl_analytics
            tpsl_reports = run_tpsl_analytics(rows, cached, output_dir=REPLAY_OUTPUT_DIR)
            for rname, data in tpsl_reports.items():
                if rname.endswith("_md_content"):
                    base = rname[: -len("_md_content")]
                    md_out = os.path.join(REPLAY_OUTPUT_DIR, f"replay_{base}.md")
                    with open(md_out, "w", encoding="utf-8") as f:
                        f.write(data)
                    _state["reports"][base + "_md"] = {
                        "rows": 1, "path": md_out, "generated_at": gen_at,
                    }
                    tpsl_files.append(f"replay_{base}.md")
                elif isinstance(data, list):
                    _save(rname, data)
                    tpsl_files.append(f"replay_{rname}.csv")
                elif isinstance(data, dict) and data.get("streamed"):
                    # tpsl_trades was streamed directly to disk by the engine
                    _state["reports"][rname] = {
                        "rows": data.get("rows", 0),
                        "path": data.get("path", ""),
                        "generated_at": gen_at,
                    }
                    tpsl_n_trades = int(data.get("rows", 0))
                    tpsl_files.append(f"replay_{rname}.csv")
            tpsl_status = "ok"
            log.info("TP/SL analytics completed: %d trades, %d files written",
                     tpsl_n_trades, len(tpsl_files))
        except Exception as _tpsl_e:
            log.warning("TP/SL analytics failed (non-fatal): %s", _tpsl_e)
            tpsl_status = "failed"
            _save("tpsl_validation", [{
                "validation_name": "engine_error",
                "status":          "FAIL",
                "details":         str(_tpsl_e),
            }])
            tpsl_files = ["replay_tpsl_validation.csv"]

        cached["tpsl_status"]   = tpsl_status
        cached["tpsl_n_trades"] = tpsl_n_trades
        cached["tpsl_files"]    = tpsl_files

        # 19 — Summary (after TP/SL so it can include accurate TP/SL section)
        _state["progress"] = 19; _state["message"] = "Writing summary..."
        md = _md_summary(cached, gen_at, tf, universe, len(rows))
        md_path = os.path.join(REPLAY_OUTPUT_DIR, "replay_summary.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md)
        _state["reports"]["summary_md"] = {"rows": 1, "path": md_path, "generated_at": gen_at}

        _state.update(status="completed", completed_at=_now(), message="Done.")

    except Exception as e:
        log.exception("Replay analytics error")
        _state.update(status="failed", error=str(e), message=str(e))


# ─── Read-back helpers ─────────────────────────────────────────────────────────

def get_report_list() -> List[dict]:
    out = []
    if not os.path.exists(REPLAY_OUTPUT_DIR):
        return out
    for fname in sorted(os.listdir(REPLAY_OUTPUT_DIR)):
        if not (fname.startswith("replay_") and (fname.endswith(".csv") or fname.endswith(".md"))):
            continue
        path = os.path.join(REPLAY_OUTPUT_DIR, fname)
        name = fname.removeprefix("replay_").removesuffix(".csv").removesuffix(".md")
        try:
            size  = os.path.getsize(path)
            mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%dT%H:%M:%S")
            with open(path, encoding="utf-8") as f:
                rows = max(0, sum(1 for _ in f) - 1)
        except Exception:
            size = mtime = rows = 0
        out.append({"name": name, "filename": fname, "rows": rows,
                    "size_bytes": size, "generated_at": mtime})
    return out


def load_report(name: str, page: int = 1, page_size: int = 500) -> Tuple[Optional[dict], Optional[str]]:
    # Markdown reports (summary_md, tpsl_summary_md, tpsl_implementation_audit_md, …)
    md_path = os.path.join(REPLAY_OUTPUT_DIR, f"replay_{name}.md")
    if name.endswith("_md") and os.path.exists(md_path):
        with open(md_path, encoding="utf-8") as f:
            return {"type": "markdown", "content": f.read()}, None

    path = os.path.join(REPLAY_OUTPUT_DIR, f"replay_{name}.csv")
    if not os.path.exists(path):
        return None, f"Report not found: {name}"
    try:
        with open(path, newline="", encoding="utf-8") as f:
            all_rows = list(csv.DictReader(f))
        total = len(all_rows)
        start = (page - 1) * page_size
        return {
            "type":      "table",
            "rows":      all_rows[start:start + page_size],
            "total":     total,
            "page":      page,
            "page_size": page_size,
            "pages":     max(1, (total + page_size - 1) // page_size),
        }, None
    except Exception as e:
        return None, str(e)


def export_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(REPLAY_OUTPUT_DIR):
            for fname in os.listdir(REPLAY_OUTPUT_DIR):
                fpath = os.path.join(REPLAY_OUTPUT_DIR, fname)
                if os.path.isfile(fpath):
                    zf.write(fpath, fname)
    return buf.getvalue()
