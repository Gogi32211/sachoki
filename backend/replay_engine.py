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
    "total_steps":  15,
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
    path = os.path.join(STOCK_STAT_DIR, f"stock_stat_{universe}_{tf}.csv")
    if not os.path.exists(path):
        return None, f"File not found: {path}. Run Stock Stat first."
    rows = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append(dict(row))
    except Exception as e:
        return None, str(e)
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
    "HAS_ELITE_MODEL","HAS_BEAR_MODEL",
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


# ─── Compute replay labels ─────────────────────────────────────────────────────

def _label_rows(rows: List[dict]) -> List[dict]:
    for r in rows:
        ret1  = _n(r, "RET_1D")
        ret3  = _n(r, "RET_3D")
        ret5  = _n(r, "RET_5D")
        ret10 = _n(r, "RET_10D")
        max5  = _n(r, "MAX_RET_5D")
        max10 = _n(r, "MAX_RET_10D")
        fbs   = _n(r, "FINAL_BULL_SCORE")
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

        r["_missed"]  = r["_bw10"] and fbs < 60 and hbs < 40
        r["_fp"]      = fbs >= 100 and r["_fail10"]
        r["_ls_win"]  = fbs < 40  and r["_bw10"]
        r["_hs_win"]  = fbs >= 100 and r["_bw10"]
        r["_hs_fail"] = fbs >= 100 and r["_fail10"]
        r["_rkt_miss"]= r["_para"] and rocket < 25
        r["_bear_win"]= _str(r, "FINAL_REGIME") == "BEARISH_PHASE" and r["_bw10"]
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

_SCORE_COLS = [
    ("FINAL_BULL_SCORE",      "FINAL_BULL"),
    ("SIGNAL_SCORE",          "SIGNAL"),
    ("turbo_score",           "TURBO"),
    ("CLEAN_ENTRY_SCORE",     "CLEAN_ENTRY"),
    ("SHAKEOUT_ABSORB_SCORE", "SHAKEOUT_ABSORB"),
    ("ROCKET_SCORE",          "ROCKET"),
    ("HARD_BEAR_SCORE",       "HARD_BEAR"),
    ("BEARISH_RISK_SCORE",    "BEARISH_RISK"),
    ("rtb_total",             "RTB"),
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


# ─── Section 7: Missed big winners ────────────────────────────────────────────

def _miss_reason(r: dict) -> str:
    fbs    = _f(r.get("FINAL_BULL_SCORE", 0))
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
    if not _f(r.get("HAS_ELITE_MODEL", 0)):
        parts.append("D:no_named_model")
    return "|".join(parts) if parts else "J:unknown"

def missed_winners(rows: List[dict], top_n: int = 500) -> List[dict]:
    missed = sorted([r for r in rows if r["_missed"]], key=lambda r: -r["_max10"])
    out = []
    for r in missed[:top_n]:
        out.append({
            "ticker":           _str(r, "ticker"),
            "date":             _str(r, "date"),
            "close":            _n(r, "close"),
            "final_bull_score": _n(r, "FINAL_BULL_SCORE"),
            "turbo_score":      _n(r, "turbo_score"),
            "signal_score":     _n(r, "SIGNAL_SCORE"),
            "rtb_score":        _n(r, "rtb_total"),
            "final_regime":     _str(r, "FINAL_REGIME"),
            "final_score_bucket": _str(r, "FINAL_SCORE_BUCKET"),
            "already_extended": int(r["_ext"]),
            "ret_3d":           r["_ret3"],
            "ret_5d":           r["_ret5"],
            "ret_10d":          r["_ret10"],
            "max_high_5d":      r["_max5"],
            "max_high_10d":     r["_max10"],
            "active_signals":   "|".join(_active_sigs(r)),
            "likely_miss_reason": _miss_reason(r),
        })
    return out


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
            "final_bull_score":   _n(r, "FINAL_BULL_SCORE"),
            "final_regime":       _str(r, "FINAL_REGIME"),
            "final_score_bucket": _str(r, "FINAL_SCORE_BUCKET"),
            "hard_bear_score":    _n(r, "HARD_BEAR_SCORE", "BEARISH_RISK_SCORE"),
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
        is_bear = component == "HARD_BEAR_SCORE"
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
        ("FINAL_BULL_SCORE < 40",  lambda r: _f(r.get("FINAL_BULL_SCORE",0)) < 40),
        ("FINAL_BULL_SCORE < 60",  lambda r: _f(r.get("FINAL_BULL_SCORE",0)) < 60),
        ("BEARISH_PHASE_REGIME",   lambda r: _str(r,"FINAL_REGIME") == "BEARISH_PHASE"),
        ("NO_ELITE_MODEL",         lambda r: not _f(r.get("HAS_ELITE_MODEL",0))),
        ("HARD_BEAR >= 40",        lambda r: _f(r.get("HARD_BEAR_SCORE",r.get("BEARISH_RISK_SCORE",0))) >= 40),
        ("NEUTRAL_OR_LOW_REGIME",  lambda r: _str(r,"FINAL_REGIME") in ("NEUTRAL_OR_LOW","","NONE")),
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

def _fetch_splits(tickers: List[str], min_date: str) -> List[dict]:
    """Try yfinance for split events; returns empty list on any failure."""
    events = []
    try:
        import yfinance as yf
        for t in tickers:
            try:
                splits = yf.Ticker(t).splits
                if splits is None or len(splits) == 0:
                    continue
                for dt, ratio in splits.items():
                    ds = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
                    if ds < min_date:
                        continue
                    events.append({
                        "ticker": t, "split_date": ds,
                        "split_ratio": float(ratio),
                        "split_type": "FORWARD_SPLIT" if ratio > 1 else
                                      ("REVERSE_SPLIT" if ratio < 1 else "UNKNOWN"),
                    })
            except Exception:
                pass
    except ImportError:
        pass
    return events


def split_analytics(rows: List[dict]) -> dict:
    tickers = list({_str(r, "ticker") for r in rows if _str(r, "ticker")})
    dates   = sorted({_str(r, "date") for r in rows if _str(r, "date")})
    if not dates or not tickers:
        return {"available": False, "message": "No ticker/date data in dataset.",
                "events": [], "missed": [], "false_positives": []}

    min_date = dates[0]
    split_evts = _fetch_splits(tickers, min_date)

    if not split_evts:
        return {
            "available": False,
            "message": "No split events found via yfinance in dataset date range. "
                       "Connect yfinance or add split data to enable split analysis.",
            "events": [], "missed": [], "false_positives": [],
        }

    # Build ticker→date→row index
    by_td: Dict[str, Dict[str, dict]] = {}
    for r in rows:
        t = _str(r, "ticker"); d = _str(r, "date")
        if t and d:
            by_td.setdefault(t, {})[d] = r

    event_rows = []
    for evt in split_evts:
        t  = evt["ticker"]
        sd = evt["split_date"]
        tr = by_td.get(t, {})
        # Find nearest row to split date
        sr = tr.get(sd) or (tr.get(min(tr.keys(), key=lambda d: abs(d.replace("-","") + "0" if len(d) < 10 else d))) if tr else None)
        if sr is None:
            continue
        sc = _n(sr, "close")
        if sc <= 0:
            continue
        row = {
            **evt,
            "split_day_close":    sc,
            "final_bull_score":   _n(sr, "FINAL_BULL_SCORE"),
            "turbo_score":        _n(sr, "turbo_score"),
            "signal_score":       _n(sr, "SIGNAL_SCORE"),
            "rtb_score":          _n(sr, "rtb_total"),
            "clean_entry_score":  _n(sr, "CLEAN_ENTRY_SCORE"),
            "rocket_score":       _n(sr, "ROCKET_SCORE"),
            "hard_bear_score":    _n(sr, "HARD_BEAR_SCORE","BEARISH_RISK_SCORE"),
            "final_regime":       _str(sr, "FINAL_REGIME"),
            "final_score_bucket": _str(sr, "FINAL_SCORE_BUCKET"),
            "post_split_ret_5d":  sr.get("_ret5",  _f(sr.get("RET_5D",  0))),
            "post_split_ret_10d": sr.get("_ret10", _f(sr.get("RET_10D", 0))),
            "post_split_max5d":   sr.get("_max5",  _f(sr.get("MAX_RET_5D",  0))),
            "post_split_max10d":  sr.get("_max10", _f(sr.get("MAX_RET_10D", 0))),
            "post_split_breakout_10d":  int(sr.get("_max10", _f(sr.get("MAX_RET_10D",0))) >= 20.0),
            "post_split_parabolic_30d": int(sr.get("_max10", _f(sr.get("MAX_RET_10D",0))) >= 30.0),
            "post_split_fail_10d":      int(sr.get("_fail10", _f(sr.get("RET_10D",0)) <= -12.0)),
            "active_signals_split_day": "|".join(_active_sigs(sr)),
        }
        event_rows.append(row)

    missed = [e for e in event_rows if e["post_split_breakout_10d"] and e["final_bull_score"] < 60]
    fps    = [e for e in event_rows if e["final_bull_score"] >= 100 and e["post_split_fail_10d"]]
    fwd    = [e for e in event_rows if e["split_type"] == "FORWARD_SPLIT"]
    rev    = [e for e in event_rows if e["split_type"] == "REVERSE_SPLIT"]
    msg    = (f"Analyzed {len(event_rows)} split events "
              f"({len(fwd)} forward, {len(rev)} reverse). "
              f"Missed breakouts: {len(missed)}. Split false positives: {len(fps)}.")
    return {
        "available": True,
        "message": msg,
        "events": event_rows,
        "missed": missed,
        "false_positives": fps,
    }


# ─── Markdown summary ─────────────────────────────────────────────────────────

def _md_summary(reports: dict, gen_at: str, tf: str, universe: str, n: int) -> str:
    lines = [
        "# Replay Analytics Summary",
        f"",
        f"Generated: {gen_at}  ",
        f"Dataset: **{universe}** / **{tf}** — {n:,} rows",
        "",
        "---",
        "",
        "## 1. Executive Summary",
        "",
    ]

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

    # Missed reasons
    mw = reports.get("missed_winners", [])
    if mw:
        from collections import Counter
        reason_counts: Counter = Counter()
        for r in mw:
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

    lines += [
        "", "---", "",
        "## 24. Recommended next steps", "",
        "1. Review **Scored But Weak** table for signals to reduce/remove",
        "2. Review **Active Unscored** table for signals to add",
        "3. Check **Missed Big Winners** for regime/filter issues",
        "4. Check **False Positives** for volatility/extension risk gaps",
        "5. If split data is available, review **Split Breakout Analysis**",
        "",
        "*Generated by Sachoki Replay Analytics Engine*",
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
        # 1 — Load
        _state["progress"] = 1; _state["message"] = "Loading stock_stat CSV..."
        rows, err = _load_stock_stat(tf, universe)
        if err:
            raise RuntimeError(err)
        _state["row_count"] = len(rows)

        # 2 — Label
        _state["progress"] = 2; _state["message"] = "Computing replay labels..."
        rows = _label_rows(rows)

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

        # 9 — Missed winners
        _state["progress"] = 9; _state["message"] = "Missed big winners..."
        cached["missed_winners"] = _save("missed_winners", missed_winners(rows))

        # 10 — False positives
        _state["progress"] = 10; _state["message"] = "False positives..."
        cached["false_positives"] = _save("false_positives", false_positives(rows))

        # 11 — Unscored
        _state["progress"] = 11; _state["message"] = "Active unscored signals..."
        _save("unscored_signals", unscored_signals(rows, min_count=20))

        # 12 — Scored weak
        _state["progress"] = 12; _state["message"] = "Scored but weak signals..."
        _save("scored_weak", scored_weak(rows, min_count=20))

        # 13 — Filter audit
        _state["progress"] = 13; _state["message"] = "Filter miss audit..."
        _save("filter_miss_audit", filter_miss_audit(rows))

        # 14 — Splits
        _state["progress"] = 14; _state["message"] = "Split analytics (fetching split data)..."
        sr = split_analytics(rows)
        _save("split_events", sr.get("events", []))
        _save("split_missed", sr.get("missed", []))
        _save("split_false_positives", sr.get("false_positives", []))
        cached["split_analytics"] = sr

        # 15 — Summary
        _state["progress"] = 15; _state["message"] = "Writing summary..."
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
    # Special case: summary markdown
    md_path = os.path.join(REPLAY_OUTPUT_DIR, f"replay_{name}.md")
    if name == "summary_md" and os.path.exists(md_path):
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
