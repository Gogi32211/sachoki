"""
studio/scoring_lab.py — Custom scoring lab.

Lets users define custom signal weights and hard filters,
then backtest against historical events to compare with turbo_score.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Optional

import numpy as np
import pandas as pd

from studio.db import get_conn

log = logging.getLogger(__name__)


def define_score(
    name:         str,
    weights:      dict[str, float],   # { "l34": 8, "ad_cluster": 15, ... }
    hard_filters: list[dict] | None = None,
    threshold:    int = 45,
) -> str:
    """
    Save a custom score definition to DB.
    Returns score_id.
    """
    score_id = f"{name.lower().replace(' ', '_')}_{str(uuid.uuid4())[:6]}"
    conn = get_conn()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO custom_scores (score_id, name, weights, hard_filters, threshold) VALUES (?, ?, ?, ?, ?)",
            [score_id, name, json.dumps(weights), json.dumps(hard_filters or []), threshold],
        )
        conn.commit()
    finally:
        conn.close()
    log.info("Defined custom score: %s (%s)", name, score_id)
    return score_id


def apply_custom_score(df: pd.DataFrame, weights: dict, hard_filters: list, threshold: int) -> pd.Series:
    """
    Compute custom score for each row in df.
    weights:      { col_name: weight_value }
    hard_filters: [ { "if_col": ..., "if_val": ..., "action": "zero" | "multiply", "factor": 0.3 } ]
    """
    score = pd.Series(0.0, index=df.index)

    for col, w in weights.items():
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").fillna(0)
            score += vals * w

    # Apply hard filters
    for hf in (hard_filters or []):
        col = hf.get("if_col", "")
        val = hf.get("if_val", 1)
        action = hf.get("action", "zero")
        factor = hf.get("factor", 0.0)

        if col not in df.columns:
            continue
        col_vals = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if val == 1:
            mask = col_vals >= 1
        else:
            mask = col_vals == val

        if action == "zero":
            score[mask] = 0.0
        elif action == "multiply":
            score[mask] *= factor

    score = score.clip(lower=0)
    return score


def backtest_score(
    score_id:   str,
    event_type: str = "BULL_2X_60D",
    date_from:  Optional[str] = None,
    date_to:    Optional[str] = None,
    universes:  list[str] | None = None,
) -> dict:
    """
    Backtest a custom score against historical events.
    Compares precision/recall/f1 and avg forward returns vs turbo_score.
    """
    # Load score definition
    conn = get_conn(read_only=True)
    try:
        row = conn.execute(
            "SELECT name, weights, hard_filters, threshold FROM custom_scores WHERE score_id = ?",
            [score_id],
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return {"error": f"Score {score_id} not found"}

    score_name, weights_json, filters_json, threshold = row
    weights      = json.loads(weights_json)
    hard_filters = json.loads(filters_json)

    # Load bars
    conn = get_conn(read_only=True)
    try:
        available = conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist()
        # Select all needed columns
        need_cols = (
            ["ticker", "date", "turbo_score", "universe"] +
            [c for c in weights.keys() if c in available] +
            [c for c in (hf.get("if_col","") for hf in hard_filters) if c in available] +
            ["fwd_5d", "fwd_10d", "fwd_20d", "fwd_60d",
             "mfe_20d", "mfe_60d", "hit_2x_60d", "hit_50pct_20d"]
        )
        need_cols = list(dict.fromkeys(c for c in need_cols if c in available))
        sel = ", ".join(need_cols)

        where_parts = ["turbo_score IS NOT NULL", "fwd_10d IS NOT NULL"]
        params: list = []
        if date_from:
            where_parts.append("date >= ?"); params.append(date_from)
        if date_to:
            where_parts.append("date <= ?"); params.append(date_to)
        if universes:
            where_parts.append(f"universe IN ({', '.join('?' * len(universes))})")
            params.extend(universes)

        df = conn.execute(
            f"SELECT {sel} FROM bars WHERE {' AND '.join(where_parts)}",
            params,
        ).fetchdf()
    finally:
        conn.close()

    if len(df) == 0:
        return {"error": "No bars found for given filters"}

    log.info("Backtest: %d bars loaded for score=%s", len(df), score_name)

    # Compute custom score
    df["custom_score"] = apply_custom_score(df, weights, hard_filters, threshold)
    df["custom_signal"] = (df["custom_score"] >= threshold).astype(int)
    df["turbo_signal"]  = (pd.to_numeric(df["turbo_score"], errors="coerce").fillna(0) >= threshold).astype(int)

    # Ground truth: did the event happen?
    if "hit_2x_60d" in df.columns:
        df["gt"] = pd.to_numeric(df["hit_2x_60d"], errors="coerce").fillna(0).astype(int)
    elif "hit_50pct_20d" in df.columns:
        df["gt"] = pd.to_numeric(df["hit_50pct_20d"], errors="coerce").fillna(0).astype(int)
    else:
        df["gt"] = (pd.to_numeric(df.get("fwd_20d", pd.Series(0, index=df.index)), errors="coerce").fillna(0) >= 40).astype(int)

    def _metrics(signal_col: str) -> dict:
        tp = int(((df[signal_col] == 1) & (df["gt"] == 1)).sum())
        fp = int(((df[signal_col] == 1) & (df["gt"] == 0)).sum())
        fn = int(((df[signal_col] == 0) & (df["gt"] == 1)).sum())
        prec   = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1     = 2 * prec * recall / (prec + recall) if (prec + recall) > 0 else 0.0
        # Forward returns for rows where signal fired
        fired = df[df[signal_col] == 1]
        result = {
            "precision": round(prec * 100, 1),
            "recall":    round(recall * 100, 1),
            "f1":        round(f1 * 100, 1),
            "tp": tp, "fp": fp, "fn": fn,
            "signal_count": int(df[signal_col].sum()),
        }
        for fcol in ["fwd_5d","fwd_10d","fwd_20d","fwd_60d"]:
            if fcol in fired.columns:
                vals = pd.to_numeric(fired[fcol], errors="coerce").dropna()
                result[f"avg_{fcol}"]  = round(vals.mean(), 2)
                result[f"win_{fcol}"]  = round((vals > 0).mean() * 100, 1)
        return result

    turbo_m  = _metrics("turbo_signal")
    custom_m = _metrics("custom_signal")

    comparison = {}
    for key in ["precision","recall","f1","avg_fwd_5d","avg_fwd_10d","avg_fwd_20d","avg_fwd_60d","win_fwd_5d"]:
        tv = turbo_m.get(key, 0) or 0
        cv = custom_m.get(key, 0) or 0
        comparison[key] = {
            "turbo":  tv,
            "custom": cv,
            "delta":  round(cv - tv, 2),
            "better": cv > tv,
        }

    result = {
        "score_id":   score_id,
        "score_name": score_name,
        "threshold":  threshold,
        "event_type": event_type,
        "n_bars":     len(df),
        "n_events":   int(df["gt"].sum()),
        "turbo":      turbo_m,
        "custom":     custom_m,
        "comparison": comparison,
    }

    # Save to DB
    conn = get_conn()
    try:
        conn.execute("""
            INSERT INTO backtest_results
              (score_id, event_type, date_from, date_to, universe,
               precision_, recall_, f1_, avg_fwd_20d, avg_fwd_60d,
               fp_rate, missed_count, caught_count, result_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            score_id, event_type, date_from, date_to,
            ",".join(universes or ["all"]),
            custom_m.get("precision", 0), custom_m.get("recall", 0),
            custom_m.get("f1", 0),
            custom_m.get("avg_fwd_20d", 0), custom_m.get("avg_fwd_60d", 0),
            custom_m.get("fp", 0) / max(custom_m.get("signal_count", 1), 1),
            custom_m.get("fn", 0), custom_m.get("tp", 0),
            json.dumps(result),
        ])
        conn.commit()
    finally:
        conn.close()

    return result


def list_scores() -> list[dict]:
    conn = get_conn(read_only=True)
    try:
        return conn.execute(
            "SELECT score_id, name, threshold, created_at FROM custom_scores ORDER BY created_at DESC"
        ).fetchdf().to_dict("records")
    finally:
        conn.close()
