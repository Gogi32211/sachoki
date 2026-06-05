"""
backtest.py — walk-forward backtest of all enumerated combos.

For each combo, runs one DuckDB query per period (train / OOS) that aggregates
forward stats over rows matching the combo's predicate. We batch combos via
conditional aggregates (one query computes many combos at once) to keep the
runtime sane on 8M+ bar rows.

Statistical discipline:
  - n_train ≥ N_MIN_TRAIN, n_oos ≥ N_MIN_OOS
  - Binomial test of OOS HH5 against same-period baseline HH5
  - Bonferroni correction by K = number of combos tested
  - PASS only if: bonferroni_p < 0.05 AND oos_hh_edge ≥ OOS_LIFT_RATIO * train_hh_edge
    AND oos_hh_edge > 0.

This keeps "real" combos and rejects in-sample-only flukes.
"""
from __future__ import annotations

import math
import time
import logging
from datetime import datetime, timezone

import duckdb

from ai_journal.db import get_analytics_conn, get_journal_conn, ensure_schema
from .enumerate import enumerate_combos

log = logging.getLogger(__name__)

# Defaults (overridable via run_walk_forward args).
TRAIN_PERIOD = ("2021-01-01", "2024-12-31")
OOS_PERIOD   = ("2025-01-01", "2026-06-04")
POP_FILTER   = "fwd_5d IS NOT NULL AND fwd_5d BETWEEN -90 AND 500"
N_MIN_TRAIN  = 200
N_MIN_OOS    = 50
OOS_LIFT_RATIO = 0.5      # OOS edge must be ≥ 50% of train edge (no big collapse)
BONFERRONI_P = 0.05
BATCH_SIZE   = 20         # combos per SQL; 5 aggs each → 100 FILTER exprs / pass. Bigger batches scale poorly on 8M rows.


def _binom_p(k: int, n: int, p: float) -> float:
    """One-sided binomial test p-value: P(X >= k | n trials, p base). Normal
    approx (n*p, n*p*(1-p)) — fine for our n ≥ 50. Returns p in [0,1]."""
    if n <= 0 or p <= 0 or p >= 1:
        return 1.0
    mu, sd = n * p, math.sqrt(n * p * (1 - p))
    if sd == 0:
        return 1.0
    z = (k - 0.5 - mu) / sd       # continuity-corrected
    # 1-CDF(z) of standard normal
    return 0.5 * math.erfc(z / math.sqrt(2))


def _period_baseline(conn, start: str, end: str) -> dict:
    """Universe-wide baseline rates within a period — same population we'll
    compare each combo against (no survivorship/leak)."""
    r = conn.execute(f"""
        SELECT count(*) n,
               avg(CASE WHEN next_pivot_is_hh_5 THEN 1.0 ELSE 0 END) hh5,
               avg(CASE WHEN fwd_5d >= 5 THEN 1.0 ELSE 0 END) big5,
               median(fwd_5d) fwd5_med,
               avg(CASE WHEN fwd_5d > 0 THEN 1.0 ELSE 0 END) win5
        FROM bars WHERE date BETWEEN ? AND ? AND {POP_FILTER}
    """, [start, end]).fetchone()
    return {"n": int(r[0]), "hh5": float(r[1] or 0), "big5": float(r[2] or 0),
            "fwd5_med": float(r[3] or 0), "win5": float(r[4] or 0)}


def _batch_metrics(conn, combos: list[dict], start: str, end: str) -> dict:
    """One DuckDB pass: per-combo n, HH5_count, big5, fwd5_med, win5 within [start,end]."""
    if not combos:
        return {}
    aggs = []
    for c in combos:
        cid = c["combo_id"]
        cond = c["sql"]
        f = f"FILTER (WHERE ({cond}))"
        aggs += [
            f"count(*) {f} AS n_{cid}",
            f"sum(CASE WHEN next_pivot_is_hh_5 THEN 1 ELSE 0 END) {f} AS hh_{cid}",
            f"sum(CASE WHEN fwd_5d >= 5 THEN 1 ELSE 0 END) {f} AS big_{cid}",
            f"median(fwd_5d) {f} AS fwd_{cid}",
            f"sum(CASE WHEN fwd_5d > 0 THEN 1 ELSE 0 END) {f} AS win_{cid}",
        ]
    sql = f"SELECT {', '.join(aggs)} FROM bars WHERE date BETWEEN ? AND ? AND {POP_FILTER}"
    row = conn.execute(sql, [start, end]).fetchone()
    out = {}
    for i, c in enumerate(combos):
        n = int(row[i * 5] or 0)
        hh = int(row[i * 5 + 1] or 0)
        big = int(row[i * 5 + 2] or 0)
        fwdm = float(row[i * 5 + 3] or 0) if row[i * 5 + 3] is not None else None
        win = int(row[i * 5 + 4] or 0)
        out[c["combo_id"]] = {"n": n, "hh_count": hh, "big_count": big,
                              "fwd5_med": fwdm, "win_count": win}
    return out


def run_walk_forward(sizes=(1, 2, 3), train=TRAIN_PERIOD, oos=OOS_PERIOD,
                     persist: bool = True) -> dict:
    ensure_schema()
    t0 = time.time()
    combos = enumerate_combos(sizes=sizes)
    K = len(combos)
    log.info("walk-forward: enumerated %d combos (sizes=%s)", K, sizes)

    a = get_analytics_conn()
    try:
        base_train = _period_baseline(a, *train)
        base_oos   = _period_baseline(a, *oos)
        log.info("baseline TRAIN n=%d HH5=%.1f%% | OOS n=%d HH5=%.1f%%",
                 base_train["n"], base_train["hh5"] * 100,
                 base_oos["n"],   base_oos["hh5"] * 100)

        results = []
        for i in range(0, K, BATCH_SIZE):
            chunk = combos[i:i + BATCH_SIZE]
            tm = _batch_metrics(a, chunk, *train)
            om = _batch_metrics(a, chunk, *oos)
            for c in chunk:
                t_ = tm[c["combo_id"]]
                o_ = om[c["combo_id"]]
                results.append({**c, "train": t_, "oos": o_})
            if ((i // BATCH_SIZE) + 1) % 10 == 0:
                log.info("  processed %d/%d combos (%.0fs)", i + len(chunk), K, time.time() - t0)
    finally:
        a.close()

    # ── decide pass/reject ───────────────────────────────────────────────────
    passed = []
    for r in results:
        nt, no = r["train"]["n"], r["oos"]["n"]
        if nt < N_MIN_TRAIN or no < N_MIN_OOS:
            r["status"] = "rejected"; r["pass_reason"] = f"low_n (train={nt}, oos={no})"
            continue
        train_hh = r["train"]["hh_count"] / nt
        oos_hh   = r["oos"]["hh_count"]  / no
        train_edge = (train_hh - base_train["hh5"]) * 100
        oos_edge   = (oos_hh   - base_oos["hh5"])   * 100
        p_value = _binom_p(r["oos"]["hh_count"], no, base_oos["hh5"])
        bonf_p  = min(1.0, p_value * K)
        r["train_hh5"] = train_hh; r["oos_hh5"] = oos_hh
        r["train_hh_edge"] = train_edge; r["oos_hh_edge"] = oos_edge
        r["p_value"] = p_value; r["bonferroni_p"] = bonf_p
        ok_p = bonf_p < BONFERRONI_P
        ok_oos_edge = oos_edge > 0 and (train_edge <= 0 or oos_edge >= OOS_LIFT_RATIO * train_edge)
        if ok_p and ok_oos_edge:
            r["status"] = "passed"; r["pass_reason"] = "ok"
            passed.append(r)
        else:
            r["status"] = "rejected"
            r["pass_reason"] = ("bonferroni p too high" if not ok_p
                                else f"oos edge collapsed (train {train_edge:+.1f}pp → oos {oos_edge:+.1f}pp)")

    log.info("walk-forward: %d/%d passed (%.0fs)", len(passed), K, time.time() - t0)

    if persist:
        _persist(results, K, base_train, base_oos, train, oos)

    return {
        "total": K,
        "passed": len(passed),
        "duration_sec": round(time.time() - t0, 1),
        "base_train_hh5": round(base_train["hh5"] * 100, 1),
        "base_oos_hh5":   round(base_oos["hh5"] * 100, 1),
        "top": sorted(passed, key=lambda r: -r["oos_hh_edge"])[:15],
    }


def _persist(results: list[dict], K: int, base_train: dict, base_oos: dict,
             train: tuple, oos: tuple):
    j = get_journal_conn()
    try:
        j.execute("DELETE FROM combo_catalog")
        for r in results:
            t_ = r.get("train", {}); o_ = r.get("oos", {})
            nt, no = t_.get("n", 0), o_.get("n", 0)
            j.execute("""INSERT INTO combo_catalog
                (combo_id, predicates, size, n_train, train_hh5, train_big5, train_fwd5_med, train_win5,
                 base_hh5_train, train_hh_edge, p_value, bonferroni_p,
                 n_oos, oos_hh5, oos_big5, oos_fwd5_med, base_hh5_oos, oos_hh_edge,
                 status, pass_reason, train_period, oos_period, created_at)
                VALUES (?,?,?,?,?,?,?,?, ?,?,?,?, ?,?,?,?,?,?, ?,?,?,?, current_timestamp)""",
                [r["combo_id"], ",".join(r["predicates"]), r["size"],
                 nt,
                 (t_.get("hh_count", 0) / nt) if nt else None,
                 (t_.get("big_count", 0) / nt) if nt else None,
                 t_.get("fwd5_med"),
                 (t_.get("win_count", 0) / nt) if nt else None,
                 base_train["hh5"], r.get("train_hh_edge"),
                 r.get("p_value"), r.get("bonferroni_p"),
                 no,
                 (o_.get("hh_count", 0) / no) if no else None,
                 (o_.get("big_count", 0) / no) if no else None,
                 o_.get("fwd5_med"),
                 base_oos["hh5"], r.get("oos_hh_edge"),
                 r.get("status"), r.get("pass_reason"),
                 f"{train[0]}/{train[1]}", f"{oos[0]}/{oos[1]}"])
        j.commit()
    finally:
        j.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import json, sys
    sizes = tuple(int(s) for s in (sys.argv[1].split(",") if len(sys.argv) > 1 else ("1", "2", "3")))
    res = run_walk_forward(sizes=sizes)
    print(json.dumps({k: v for k, v in res.items() if k != "top"}, indent=2, default=str))
    print(f"\n=== TOP {len(res['top'])} passing combos (by OOS HH-edge) ===")
    for r in res["top"]:
        print(f"  {','.join(r['predicates'])[:60]:62} n_train={r['train']['n']:>5} "
              f"train_edge={r['train_hh_edge']:+5.1f}pp  oos_edge={r['oos_hh_edge']:+5.1f}pp  "
              f"bonf_p={r['bonferroni_p']:.2e}")
