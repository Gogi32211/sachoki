"""
enumerate_greedy.py — beam search up to depth 5 atoms, on realized P&L edge.

We DO NOT exhaustively enumerate 4- and 5-tuples (52k + 325k combos × multiple-
testing penalty would either OOM the SQL planner or kill all p-values with
Bonferroni). Instead:

  1. Compute all singles + pairs (~600) on TRAIN P&L. Keep top-K passing OOS.
  2. From each top-K parent, add ONE atom → all candidate triples. Score on
     TRAIN, keep top-K passing OOS.
  3. Repeat to depth=5.

This produces ~K * 35 * 4 ≈ 4-8k tests instead of ~400k, so a single combined
Bonferroni penalty is feasible AND every combo at depth d is GROWN from a
combo proven at depth d-1 — no rule that "needs all five together to look good"
survives, only those whose every prefix already had edge.

PASS criteria (same firewall idea as the HH version):
  - n_train >= N_MIN_TRAIN, n_oos >= N_MIN_OOS
  - oos_edge_avg > 0
  - oos_edge_avg >= 0.5 * train_edge_avg  (no big collapse)
  - bonferroni p < 0.05 (binomial on win-rate vs OOS baseline)

We persist to combo_catalog_pnl (separate table — keeps HH catalog intact for
the "what changed" comparison).
"""
from __future__ import annotations

import math
import hashlib
import time
import logging

from ai_journal.db import get_analytics_conn, get_journal_conn, ensure_schema
from .enumerate import ATOMS, _is_redundant, _EXCLUSIVE_GROUPS
from .backtest_pnl import baseline_pnl, batch_pnl, caps_for

log = logging.getLogger(__name__)

TRAIN_PERIOD = ("2021-01-01", "2024-12-31")
OOS_PERIOD   = ("2025-01-01", "2026-06-04")
HORIZON_DEFAULT = 10
N_MIN_TRAIN  = 200
N_MIN_OOS    = 50
BEAM         = 40       # top-K kept at each depth (40 × 35 next ≈ 1400 / depth)
DEPTH_MAX    = 5
OOS_LIFT_RATIO = 0.5
BONFERRONI_P = 0.05


_SCHEMA = """
CREATE TABLE IF NOT EXISTS combo_catalog_pnl (
    combo_id        VARCHAR,
    predicates      VARCHAR,
    size            INTEGER,
    horizon         INTEGER,
    n_train         BIGINT,
    train_avg_clip  DOUBLE,
    train_win       DOUBLE,
    base_avg_train  DOUBLE,
    train_edge_avg  DOUBLE,
    n_oos           BIGINT,
    oos_avg_clip    DOUBLE,
    oos_win         DOUBLE,
    base_avg_oos    DOUBLE,
    oos_edge_avg    DOUBLE,
    p_value         DOUBLE,
    bonferroni_p    DOUBLE,
    status          VARCHAR,
    pass_reason     VARCHAR,       -- why a combo passed or was rejected
    grown_from      VARCHAR,       -- parent combo_id (NULL for depth=1)
    discovered_at   TIMESTAMP,
    PRIMARY KEY (combo_id, horizon)
);
"""


def _combo_id(names: tuple[str, ...]) -> str:
    return hashlib.md5("&".join(sorted(names)).encode()).hexdigest()[:12]


def _make(names: tuple[str, ...]) -> dict:
    sql = " AND ".join(f"({ATOMS[n]})" for n in names)
    return {"combo_id": _combo_id(names), "predicates": tuple(sorted(names)),
            "size": len(names), "sql": sql}


def _binom_p(k: int, n: int, p: float) -> float:
    if n <= 0 or p <= 0 or p >= 1:
        return 1.0
    mu, sd = n * p, math.sqrt(n * p * (1 - p))
    if sd == 0:
        return 1.0
    z = (k - 0.5 - mu) / sd
    return 0.5 * math.erfc(z / math.sqrt(2))


def _score(metric: dict, base_avg: float) -> float:
    return metric["avg_clip"] - base_avg


def run_greedy(horizon: int = HORIZON_DEFAULT, depth_max: int = DEPTH_MAX,
               beam: int = BEAM, persist: bool = True) -> dict:
    ensure_schema()
    if persist:
        j = get_journal_conn()
        try:
            j.execute(_SCHEMA)
            # idempotent migration for existing tables created before pass_reason
            j.execute("ALTER TABLE combo_catalog_pnl ADD COLUMN IF NOT EXISTS pass_reason VARCHAR")
            j.commit()
            j.execute("DELETE FROM combo_catalog_pnl WHERE horizon = ?", [horizon]); j.commit()
        finally:
            j.close()

    t0 = time.time()
    a = get_analytics_conn()
    try:
        bt = baseline_pnl(a, horizon, *TRAIN_PERIOD)
        bo = baseline_pnl(a, horizon, *OOS_PERIOD)
        log.info("horizon=%dd  TRAIN base n=%d avg=%.3f win=%.1f%%  |  OOS base n=%d avg=%.3f win=%.1f%%",
                 horizon, bt["n"], bt["avg_clip"], bt["win"] * 100,
                 bo["n"], bo["avg_clip"], bo["win"] * 100)

        # Depth 1: all singles
        atoms = list(ATOMS.keys())
        depth_combos = [_make((a_,)) for a_ in atoms]
        all_results: list[dict] = []
        tests_done = 0

        for depth in range(1, depth_max + 1):
            log.info("depth=%d: evaluating %d candidates", depth, len(depth_combos))
            tm = batch_pnl(a, depth_combos, horizon, *TRAIN_PERIOD)
            om = batch_pnl(a, depth_combos, horizon, *OOS_PERIOD)
            tests_done += len(depth_combos)
            scored = []
            for c in depth_combos:
                t_ = tm[c["combo_id"]]; o_ = om[c["combo_id"]]
                if t_["n"] < N_MIN_TRAIN or o_["n"] < N_MIN_OOS:
                    rec = {**c, "train": t_, "oos": o_, "status": "rejected",
                           "pass_reason": f"low_n (train={t_['n']}, oos={o_['n']})",
                           "train_edge": None, "oos_edge": None}
                    all_results.append(rec); continue
                te = _score(t_, bt["avg_clip"])
                oe = _score(o_, bo["avg_clip"])
                wins_count = int(round(o_["win"] * o_["n"]))
                p = _binom_p(wins_count, o_["n"], bo["win"])
                rec = {**c, "train": t_, "oos": o_,
                       "train_edge": te, "oos_edge": oe,
                       "p_value": p, "tests_done": tests_done,
                       "status": "candidate"}
                all_results.append(rec)
                # keep for next-depth growth ONLY if OOS positive + train decent
                ok_oos = oe > 0 and (te <= 0 or oe >= OOS_LIFT_RATIO * te)
                if ok_oos:
                    scored.append((oe, rec))

            scored.sort(key=lambda x: -x[0])
            top = [r for _, r in scored[:beam]]
            log.info("  depth=%d kept %d (top oos edge %.4f, %d total combos scored)",
                     depth, len(top), top[0]["oos_edge"] if top else 0.0, len(scored))

            if depth == depth_max or not top:
                break

            # Build next-depth candidates: each top combo + one new atom
            next_set: dict[str, dict] = {}
            for parent in top:
                parent_set = set(parent["predicates"])
                for atom in atoms:
                    if atom in parent_set:
                        continue
                    new_names = tuple(sorted(parent_set | {atom}))
                    if _is_redundant(new_names):
                        continue
                    cid = _combo_id(new_names)
                    if cid in next_set:
                        continue
                    nc = _make(new_names)
                    nc["grown_from"] = parent["combo_id"]
                    next_set[cid] = nc
            depth_combos = list(next_set.values())

    finally:
        a.close()

    # Final Bonferroni using total tests done across all depths
    K = max(1, tests_done)
    for r in all_results:
        if r["status"] == "rejected":
            continue
        bp = min(1.0, (r.get("p_value") or 1.0) * K)
        r["bonferroni_p"] = bp
        oe = r.get("oos_edge") or 0
        te = r.get("train_edge") or 0
        ok_oos = oe > 0 and (te <= 0 or oe >= OOS_LIFT_RATIO * te)
        ok_p = bp < BONFERRONI_P
        if ok_oos and ok_p:
            r["status"] = "passed"; r["pass_reason"] = "ok"
        else:
            r["status"] = "rejected"
            if not ok_p:
                r["pass_reason"] = f"bonferroni p too high ({bp:.2e})"
            elif oe <= 0:
                r["pass_reason"] = f"oos edge non-positive ({oe:+.3f}%)"
            else:
                r["pass_reason"] = f"oos edge collapsed (train {te:+.3f}% → oos {oe:+.3f}%)"

    if persist:
        _persist(all_results, horizon, bt, bo)

    passed = [r for r in all_results if r["status"] == "passed"]
    by_size = {}
    for p in passed:
        by_size[p["size"]] = by_size.get(p["size"], 0) + 1
    log.info("greedy done: total tested=%d, passed=%d (by size=%s) in %.1fs",
             tests_done, len(passed), by_size, time.time() - t0)
    return {
        "horizon": horizon,
        "total_tested": tests_done,
        "passed": len(passed),
        "passed_by_size": by_size,
        "base_train_avg_clip": round(bt["avg_clip"], 3),
        "base_oos_avg_clip": round(bo["avg_clip"], 3),
        "duration_sec": round(time.time() - t0, 1),
        "top": sorted(passed, key=lambda r: -r["oos_edge"])[:25],
    }


def _persist(results: list[dict], horizon: int, bt: dict, bo: dict):
    j = get_journal_conn()
    try:
        for r in results:
            t_ = r.get("train", {}); o_ = r.get("oos", {})
            j.execute("""INSERT OR REPLACE INTO combo_catalog_pnl
                (combo_id, predicates, size, horizon,
                 n_train, train_avg_clip, train_win, base_avg_train, train_edge_avg,
                 n_oos, oos_avg_clip, oos_win, base_avg_oos, oos_edge_avg,
                 p_value, bonferroni_p, status, pass_reason, grown_from, discovered_at)
                VALUES (?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, current_timestamp)""",
                [r["combo_id"], ",".join(r["predicates"]), r["size"], horizon,
                 t_.get("n", 0), t_.get("avg_clip"), t_.get("win"), bt["avg_clip"], r.get("train_edge"),
                 o_.get("n", 0), o_.get("avg_clip"), o_.get("win"), bo["avg_clip"], r.get("oos_edge"),
                 r.get("p_value"), r.get("bonferroni_p"), r.get("status"),
                 r.get("pass_reason"), r.get("grown_from")])
        j.commit()
    finally:
        j.close()


if __name__ == "__main__":
    import sys, json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    h = int(sys.argv[1]) if len(sys.argv) > 1 else HORIZON_DEFAULT
    res = run_greedy(horizon=h)
    print(json.dumps({k: v for k, v in res.items() if k != "top"}, indent=2, default=str))
    print(f"\n=== TOP {len(res['top'])} P&L-passing combos at H={h}d (by OOS edge) ===")
    for r in res["top"]:
        print(f"  [{r['size']}] {','.join(r['predicates'])[:65]:67} "
              f"n_train={r['train']['n']:>6} n_oos={r['oos']['n']:>5}  "
              f"train_edge={r['train_edge']:+.4f}  oos_edge={r['oos_edge']:+.4f}  "
              f"bonf_p={r['bonferroni_p']:.2e}")
