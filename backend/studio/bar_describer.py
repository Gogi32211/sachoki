"""
studio/bar_describer.py — Rule-based bar description generator.

Generates deterministic, human-readable descriptions of bars
based on their signal columns. No LLM required — pure Python logic.
Uses the same signal columns stored in the bars table.
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)

# ── Helpers ────────────────────────────────────────────────────────────────────
def _b(row: dict, col: str) -> bool:
    """Return True if column is truthy (1, True, '1')."""
    v = row.get(col)
    if v is None:
        return False
    try:
        return float(v) >= 1
    except (ValueError, TypeError):
        return bool(v)


def _f(row: dict, col: str, decimals: int = 1) -> Optional[float]:
    """Return float value or None."""
    v = row.get(col)
    if v is None:
        return None
    try:
        return round(float(v), decimals)
    except (ValueError, TypeError):
        return None


# ── Signal name lookups ────────────────────────────────────────────────────────
_TZ_NAMES = {
    "T1G": "first-bull gap",  "T1": "first-bull",
    "T2G": "continuation gap","T2": "continuation",
    "T3":  "lower-open bull", "T4": "full engulf",
    "T5":  "weak bull",       "T6": "engulf-bull",
    "T9":  "inside-bull",     "T10": "inside-cont",
    "T11": "mid-close bull",  "T12": "lower-open cont",
    "Z1G": "first-bear gap",  "Z1": "first-bear",
    "Z2G": "bear-cont gap",   "Z2": "bear-cont",
    "Z3":  "higher-open bear","Z4": "bear engulf",
    "Z5":  "weak bear",       "Z6": "bear engulf prev",
    "Z9":  "inside-bear",     "Z10": "inside-bear cont",
    "Z11": "mid-close bear",  "Z12": "higher-open bear-cont",
}

_VOL_BUCKET_DESC = {
    "VB": "very-high-vol",
    "B":  "bullish-vol",
    "N":  "normal-vol",
    "L":  "low-vol",
    "W":  "weak-vol",
}

_RTB_PHASE_DESC = {
    "A": "RTB Accumulation",
    "B": "RTB Breakout",
    "C": "RTB Continuation",
    "D": "RTB Distribution",
}


def generate_bar_description(row: dict) -> str:
    """
    Generate a short (~200 char) description of a bar.
    Input: dict with DB column values for a single bar.
    """
    parts = []

    # ── T/Z signal ────────────────────────────────────────────────────────────
    t_sig = row.get("t_sig") or ""
    z_sig = row.get("z_sig") or ""
    if t_sig and t_sig not in ("", "None", "nan"):
        name = _TZ_NAMES.get(str(t_sig), str(t_sig))
        parts.append(f"📈 {t_sig}({name})")
    elif z_sig and z_sig not in ("", "None", "nan"):
        name = _TZ_NAMES.get(str(z_sig), str(z_sig))
        parts.append(f"📉 {z_sig}({name})")

    # ── Volume bucket ─────────────────────────────────────────────────────────
    vb = str(row.get("vol_bucket") or "").strip()
    if vb:
        parts.append(_VOL_BUCKET_DESC.get(vb, f"vol:{vb}"))

    # ── L signal ─────────────────────────────────────────────────────────────
    l_sig = row.get("l_sig") or ""
    if l_sig and l_sig not in ("", "None", "nan"):
        parts.append(f"L:{l_sig}")
    elif _b(row, "l34"):
        parts.append("L34-coil")
    elif _b(row, "l43"):
        parts.append("L43")
    elif _b(row, "l22"):
        parts.append("L22-supply")

    # ── WLNBB overlays ────────────────────────────────────────────────────────
    if _b(row, "sig_fri34"):
        parts.append("FRI34★")
    if _b(row, "sig_blue"):
        parts.append("BLUE")

    # ── Price vs EMAs ─────────────────────────────────────────────────────────
    ema_parts = []
    if _b(row, "price_gt_200"):
        ema_parts.append("↑EMA200")
    elif _b(row, "price_lt_200"):
        ema_parts.append("↓EMA200")
    if _b(row, "price_gt_89"):
        ema_parts.append("↑EMA89")
    elif _b(row, "price_lt_89"):
        ema_parts.append("↓EMA89")
    if ema_parts:
        parts.append("/".join(ema_parts))

    # ── RSI ───────────────────────────────────────────────────────────────────
    if _b(row, "rsi_le_35"):
        parts.append("RSI≤35(oversold)")
    elif _b(row, "rsi_ge_70"):
        parts.append("RSI≥70(hot)")

    # ── VABS signals ──────────────────────────────────────────────────────────
    vabs_hits = []
    for col, label in [
        ("sig_abs","ABS"), ("sig_clm","CLB"), ("vbo_up","VBO↑"),
        ("sig_bc","BC❗"), ("sig_sc","SC"), ("sig_best","BEST"),
        ("sig_strong","STRONG"),
    ]:
        if _b(row, col):
            vabs_hits.append(label)
    if vabs_hits:
        parts.append("+".join(vabs_hits))

    # ── GOG tier ─────────────────────────────────────────────────────────────
    gog = row.get("gog_tier") or ""
    if gog and gog not in ("", "None", "nan", "0"):
        parts.append(f"GOG:{gog}")

    # ── PARA ─────────────────────────────────────────────────────────────────
    if _b(row, "sig_para_retest"):
        parts.append("PARA-retest✓")
    elif _b(row, "sig_para_plus"):
        parts.append("PARA+")
    elif _b(row, "sig_para_prep"):
        parts.append("PARA-prep")

    # ── AD-fresh / cluster ────────────────────────────────────────────────────
    if _b(row, "ad_cluster"):
        parts.append("AD-CLUSTER★★")
    elif _b(row, "ad_fresh"):
        parts.append("AD-fresh★")

    # ── Wyckoff ───────────────────────────────────────────────────────────────
    wyc = str(row.get("wyc_phase") or "").strip()
    if wyc and wyc not in ("", "None", "nan", "NEUTRAL"):
        parts.append(f"WYC:{wyc}")

    # ── Prebreak ─────────────────────────────────────────────────────────────
    if _b(row, "prebreak_prime"):
        parts.append("PREBREAK-PRIME🔥")
    elif _b(row, "prebreak_ready"):
        parts.append("PREBREAK-READY")
    elif _b(row, "prebreak_watch"):
        parts.append("PREBREAK-WATCH")

    # ── Delta ────────────────────────────────────────────────────────────────
    delta_hits = []
    for col, label in [
        ("sig_dd_up_red","ΔΔ↑"), ("sig_d_up_red","Δ↑"),
        ("sig_flp_up","FLIP↑"), ("sig_org_up","ORG↑"),
        ("sig_d_dn_green","Δ↓"), ("sig_dd_dn_green","ΔΔ↓"),
    ]:
        if _b(row, col):
            delta_hits.append(label)
    if delta_hits:
        parts.append("Delta:" + "/".join(delta_hits))

    # ── EMA cross ────────────────────────────────────────────────────────────
    if _b(row, "sig_p66"):
        parts.append("P66✦")
    elif _b(row, "sig_p55"):
        parts.append("P55✦")
    elif _b(row, "sig_p89"):
        parts.append("P89")
    if _b(row, "sig_d66"):
        parts.append("D66⚠")
    elif _b(row, "sig_d55"):
        parts.append("D55⚠")

    # ── Turbo score ───────────────────────────────────────────────────────────
    ts = _f(row, "turbo_score", 0)
    if ts is not None and ts > 0:
        parts.append(f"T={int(ts)}")

    # ── RTB phase ────────────────────────────────────────────────────────────
    rtb = str(row.get("rtb_phase") or "").strip()
    if rtb and rtb not in ("", "None", "nan"):
        parts.append(_RTB_PHASE_DESC.get(rtb, f"RTB:{rtb}"))

    # ── Swing type ───────────────────────────────────────────────────────────
    sw = str(row.get("swing_type") or "").strip()
    if sw and sw not in ("", "None", "nan"):
        arrow = {"HL": "↗HL", "LL": "↕LL", "HH": "↗HH", "LH": "↘LH"}.get(sw, sw)
        parts.append(arrow)

    # ── Already extended ─────────────────────────────────────────────────────
    if _b(row, "already_extended_flag"):
        parts.append("EXTENDED⚠")

    desc = " | ".join(parts) if parts else "No signal"
    return desc[:300]


def generate_pre_move_narrative(
    bars: list[dict],
    event_date: str | None = None,
) -> str:
    """
    Generate a multi-line pre-move narrative from a list of bar dicts
    (sorted oldest-first, ending just before the event).

    bars: list of dicts (from DB, sorted by date ascending)
    """
    if not bars:
        return "No pre-window data available."

    lines = []
    n = len(bars)

    for i, bar in enumerate(bars):
        days_before = n - i
        date_str = str(bar.get("date", ""))[:10]
        ts = _f(bar, "turbo_score", 0) or 0
        close = _f(bar, "close", 2)
        desc = generate_bar_description(bar)

        # Highlight important bars
        highlight = ""
        if ts >= 60:
            highlight = " 🔥"
        elif ts >= 40:
            highlight = " ⚡"
        elif _b(bar, "ad_cluster"):
            highlight = " ★★"
        elif _b(bar, "ad_fresh") or _b(bar, "sig_fri34") or _b(bar, "l34"):
            highlight = " ★"

        close_str = f"${close:.2f}" if close else ""
        lines.append(f"  Day -{days_before:02d} [{date_str}]{highlight}: {desc} {close_str}")

    narrative = "Pre-move window (%d bars):\n" % n + "\n".join(lines)
    if event_date:
        narrative += f"\n  Day -00 [EVENT → {event_date}]: Price action started"

    return narrative


def batch_generate_descriptions(df: pd.DataFrame) -> pd.Series:
    """
    Generate descriptions for a full DataFrame.
    Returns a Series of description strings, same index as df.
    """
    log.info("Generating bar descriptions for %d rows...", len(df))
    return df.apply(lambda row: generate_bar_description(row.to_dict()), axis=1)


def get_bar_description(ticker: str, date: str) -> Optional[str]:
    """Fetch cached bar description from DB, or generate on the fly."""
    from studio.db import get_conn
    conn = get_conn(read_only=True)
    try:
        row = conn.execute(
            "SELECT bar_desc FROM bar_descriptions WHERE ticker = ? AND date = ?",
            [ticker, date],
        ).fetchone()
        if row and row[0]:
            return row[0]

        # Generate on the fly from bars table
        bar_row = conn.execute(
            "SELECT * FROM bars WHERE ticker = ? AND date = ?",
            [ticker, date],
        ).fetchdf()
        if len(bar_row) == 0:
            return None
        desc = generate_bar_description(bar_row.iloc[0].to_dict())
        return desc
    finally:
        conn.close()


def get_pre_narrative(ticker: str, event_date: str, pre_window: int = 20) -> str:
    """Generate pre-move narrative for a ticker + event date."""
    from studio.db import get_conn
    conn = get_conn(read_only=True)
    try:
        bars = conn.execute(
            """SELECT * FROM bars
               WHERE ticker = ? AND date < ?
               ORDER BY date DESC LIMIT ?""",
            [ticker, event_date, pre_window],
        ).fetchdf()

        if len(bars) == 0:
            return "No pre-window data in DB."

        bars = bars.iloc[::-1].reset_index(drop=True)  # oldest first
        bar_dicts = bars.to_dict("records")
        return generate_pre_move_narrative(bar_dicts, event_date)
    finally:
        conn.close()
