"""brain/nightly.py — the brain's daily self-learning pass (run by launchd at 11:00 Tbilisi).

Two learning steps, both apply=True (they COMMIT to the brain's own memory):
  1. calibrate()   — blend own closed-trade outcomes with historical priors → tier changes
  2. revalidate()  — re-path-sim every live edge on the fresh frame → flag/demote decayed edges

Isolated: writes ONLY brain/findings.json + brain/learning.json. Reads the DuckDB read-only, so it
is safe to run while the backend is up. Idempotent — a day with no new trades / no decay is a no-op
that just logs 'no change'. Run manually:  .venv/bin/python -m brain.nightly
"""
from __future__ import annotations
import json
import sys
import traceback
from datetime import datetime


def main() -> int:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"\n════════ brain nightly learn @ {stamp} ════════", flush=True)
    rc = 0

    # 1) outcome-learning (own trades) — cheap, no frame needed
    try:
        from brain.learn import calibrate
        c = calibrate(apply=True)
        print(f"[calibrate] {c.get('summary')}", flush=True)
        for ch in c.get("changes", []):
            print(f"           · {ch.get('edge')}: {ch.get('action')} {ch.get('from')}→{ch.get('to')} ({ch.get('why')})", flush=True)
    except Exception:
        rc = 1
        print("[calibrate] FAILED:\n" + traceback.format_exc(), flush=True)

    # 2) data-learning (re-path-sim) — warm the frame first, then revalidate
    try:
        import edge_replay as er
        er.latest_edges_map(build=True)          # warm the shared frame
        from brain.learn import revalidate
        r = revalidate(apply=True)
        print(f"[revalidate] {r.get('summary')}", flush=True)
        for e in r.get("edges", []):
            if e.get("verdict") in ("decayed", "weakened"):
                print(f"           · {e.get('edge')}: {e.get('verdict')} "
                      f"(hist {e.get('hist_median'):+.2f} → recent {e.get('recent_median'):+.2f})"
                      f"{' ' + e.get('action', '') if e.get('action') else ''}", flush=True)
    except Exception:
        rc = 1
        print("[revalidate] FAILED:\n" + traceback.format_exc(), flush=True)

    # 2b) missed-move review — the frame is already warm from step 2, so this is cheap.
    # It classifies today's top-30 gainers by WHY we did not take them and appends to the
    # ledger. It concludes nothing: top gainers are selected ON THE OUTCOME, so any state
    # found inside them looks predictive (law_single_name_conviction_is_survivorship).
    # It may only raise QUESTIONS, and only once the ledger is deep enough.
    try:
        from brain.missed import review, hypotheses
        d = review()
        cnt = ", ".join(f"{k} {v}" for k, v in sorted(d["counts"].items(), key=lambda x: -x[1]))
        print(f"[missed] {d['date']}: {d['n']} gainers (median {d['median_move']:+.1f}%) — {cnt}",
              flush=True)
        for g, n in sorted(d.get("gate_hits", {}).items(), key=lambda x: -x[1]):
            print(f"           · vetoed by {g}: {n}", flush=True)
        for q in hypotheses():
            if q.get("status") == "open":
                print(f"           ? {q['question']}", flush=True)
    except Exception:
        rc = 1
        print("[missed] FAILED:\n" + traceback.format_exc(), flush=True)

    # 3) self-discovery (HEAVY) — weekly only, on Saturdays, to avoid over-mining
    from datetime import datetime
    promoted = 0
    if datetime.now().weekday() == 5:        # Mon=0 … Sat=5
        try:
            from brain.miner import mine
            m = mine(apply=True)
            print(f"[miner] {m.get('summary')}", flush=True)
            promoted = len(m.get("promoted", []))
            for sid in m.get("promoted", []):
                print(f"           + {sid}", flush=True)
        except Exception:
            rc = 1
            print("[miner] FAILED:\n" + traceback.format_exc(), flush=True)
    else:
        print("[miner] skipped (weekly on Saturday)", flush=True)

    # 4) realized-outcome fingerprint snapshot (cheap)
    try:
        from brain.fingerprint import combo_stats
        cs = combo_stats()
        print(f"[fingerprint] {cs['n_closed']} closed trades · "
              f"{len(cs['by_signal'])} signals + {len(cs['by_pair'])} pairs with enough n", flush=True)
    except Exception:
        pass

    # health snapshot of the knowledge core
    try:
        from brain import registry
        print(f"[registry] {json.dumps(registry.summary())}", flush=True)
    except Exception:
        pass

    if promoted:                             # signal the wrapper to restart so combos go live
        print("NEEDS_RESTART=1", flush=True)
    print(f"════════ done (rc={rc}) ════════", flush=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())
