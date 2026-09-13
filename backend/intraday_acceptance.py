"""Operational gate on the fixed nightly writer — the last thing before any Massive backfill.

Restoring history before the writer is proven would only feed it back to the same job, so one real
scheduled run has to pass first. Usage:

    python intraday_acceptance.py --snapshot      # BEFORE the night's run
    python intraday_acceptance.py --verify        # AFTER it

FROZEN ACCEPTANCE (research_out/INTRADAY_WRITER_ACCEPTANCE.md):
  1. both `--tf 1h` and `--tf 4h` exit 0
  2. both print  deleted N · re-inserted M · key_difference D
  3. D < 0 on neither
  4. the cutoff-day session survives intact in the store
  5. session completeness against the 15m/1D reference calendar does not get worse
  6. zero NEW missing sessions on 1H/4H
  7. the (ticker, date) KEY SET of the recent sessions is compared, not only aggregate counts —
     a count can match while the membership has silently rotated

Read-only. Writes one JSON snapshot; touches no store.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# The baseline lives in the REPO, not under data/. `data/` is a symlink to the external
# QUANT_RESEARCH SSD, so nothing there is tracked and nothing there is readable when the volume is
# unmounted — the one thing this gate cannot afford to lose between the snapshot and the verify.
SNAP = os.environ.get("INTRADAY_ACCEPTANCE_SNAPSHOT",
                      os.path.join(ROOT, "research_out", "intraday_acceptance_snapshot.json"))
TFS = ("1h", "4h")
RECENT = 10                      # sessions whose key set is captured in full
OVERLAP = 3                      # must match update_intraday_db.OVERLAP


def assert_baseline_writable(path: str = SNAP, force: bool = False) -> None:
    """FAIL CLOSED. An existing baseline is the only link between the pre-run and post-run states;
    overwriting it with post-run data would leave the gate reporting PASS while comparing a state
    against itself. Three written warnings not to re-snapshot are not a control — this is.

    Must be called BEFORE capture(), so a refusal costs nothing and touches no store.
    """
    if os.path.exists(path) and not force:
        raise SystemExit(
            f"REFUSED: a baseline already exists at {path}\n"
            "  Overwriting it VOIDS THE ACCEPTANCE: --verify would then compare the post-run state\n"
            "  against itself and report PASS while proving nothing. The baseline is the pre-run\n"
            "  state and must not move.\n"
            "  If you genuinely mean to start a new acceptance cycle, pass --force.")


def _db(tf):
    return duckdb.connect(os.path.join(ROOT, "data", f"studio_{tf}.duckdb"), read_only=True)


def _calendar() -> list[str]:
    c = duckdb.connect(os.path.join(ROOT, "data", "studio_analytics.duckdb"), read_only=True)
    d = [str(r[0]) for r in c.execute(
        "SELECT DISTINCT CAST(date AS DATE) FROM bars WHERE universe <> 'index' ORDER BY 1").fetchall()]
    c.close()
    return d


def capture() -> dict:
    cal = _calendar()
    out = {"calendar_max": cal[-1], "tf": {}}
    for tf in TFS:
        c = _db(tf)
        per = {str(r[0]): int(r[1]) for r in c.execute(
            "SELECT CAST(date AS DATE), COUNT(DISTINCT ticker) FROM bars GROUP BY 1").fetchall()}
        recent = sorted(per)[-RECENT:]
        keys = {}
        for d in recent:
            keys[d] = sorted(r[0] for r in c.execute(
                "SELECT DISTINCT ticker FROM bars WHERE CAST(date AS DATE) = ?", [d]).fetchall())
        mx = c.execute("SELECT CAST(MAX(date) AS DATE) FROM bars").fetchone()[0]
        c.close()
        out["tf"][tf] = {"per_session": per, "max_date": str(mx), "recent_keys": keys,
                         "missing": [d for d in cal if d >= min(per) and per.get(d, 0) < 1000]}
    return out


def verify(before: dict) -> int:
    now = capture()
    cal = _calendar()
    bad = 0
    print("═" * 78)
    print("INTRADAY WRITER ACCEPTANCE — post-run verification")
    print("═" * 78)

    # FAIL CLOSED on an untested gate. If no store advanced, the nightly run never happened and this
    # is comparing the snapshot's own state against itself — every check passes and nothing is
    # proven. That is NOT a PASS; it is a scheduler/runtime finding, and the gate is still untested.
    if all(now["tf"][tf]["max_date"] == before["tf"][tf]["max_date"] for tf in TFS):
        _stuck = ", ".join(f"{tf} still {before['tf'][tf]['max_date']}" for tf in TFS)
        print(f"\n⛔ NOT TESTED — no timeframe advanced past the baseline ({_stuck}).")
        print("   The nightly run did not happen, so there is nothing to verify. This is a")
        print("   SCHEDULER / RUNTIME finding, not an acceptance result — and it is NOT a PASS.")
        print("   Do NOT backfill. Re-run --verify after a real scheduled night.")
        print("═" * 78)
        return 2
    for tf in TFS:
        b, a = before["tf"][tf], now["tf"][tf]
        print(f"\n── {tf.upper()} ──")
        print(f"  max date            {b['max_date']}  ->  {a['max_date']}")

        # 4 · the cutoff day of this run must have survived
        import datetime as _dt
        cutoff = (_dt.date.fromisoformat(b["max_date"]) - _dt.timedelta(days=OVERLAP)).isoformat()
        if cutoff in cal:
            n_b, n_a = b["per_session"].get(cutoff, 0), a["per_session"].get(cutoff, 0)
            ok = n_a >= max(1000, int(0.95 * n_b))
            print(f"  cutoff session      {cutoff}: {n_b:,} -> {n_a:,}   {'OK' if ok else '⛔ DESTROYED'}")
            bad += 0 if ok else 1
        else:
            print(f"  cutoff session      {cutoff} is not a trading day — nothing at risk this run")

        # 6 · no NEW missing session
        new_missing = sorted(set(a["missing"]) - set(b["missing"]))
        print(f"  newly missing        {new_missing if new_missing else 'none'}"
              f"   {'⛔' if new_missing else 'OK'}")
        bad += len(new_missing)

        # 5 · completeness against the reference calendar must not regress
        cov_b = sum(1 for d in cal if b["per_session"].get(d, 0) >= 1000)
        cov_a = sum(1 for d in cal if a["per_session"].get(d, 0) >= 1000)
        print(f"  complete sessions    {cov_b:,} -> {cov_a:,}   {'OK' if cov_a >= cov_b else '⛔ REGRESSED'}")
        bad += 0 if cov_a >= cov_b else 1

        # 7 · key-set comparison, not just counts
        print("  key sets on the sessions that existed before:")
        for d in sorted(b["recent_keys"]):
            kb, ka = set(b["recent_keys"][d]), set(a["recent_keys"].get(d, []))
            lost, gained = kb - ka, ka - kb
            flag = "OK" if not lost else "⛔ TICKERS LOST"
            print(f"    {d}  before {len(kb):,} · after {len(ka):,} · lost {len(lost):,} · "
                  f"gained {len(gained):,}   {flag}")
            bad += 0 if not lost else 1
    print("\n" + "═" * 78)
    print(f"ACCEPTANCE: {'PASS — the writer is safe to backfill into' if bad == 0 else f'FAIL ({bad} problems) — do NOT backfill'}")
    print("═" * 78)
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", action="store_true")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="overwrite an existing baseline — only when starting a NEW acceptance cycle")
    a = ap.parse_args()
    if a.snapshot:
        assert_baseline_writable(SNAP, a.force)      # before any store is read
        s = capture()
        json.dump(s, open(SNAP, "w"), indent=1)
        print(f"snapshot -> {SNAP}")
        for tf in TFS:
            t = s["tf"][tf]
            print(f"  {tf}: max {t['max_date']} · sessions {len(t['per_session']):,} · "
                  f"missing {len(t['missing'])} · key sets captured for {len(t['recent_keys'])} sessions")
    elif a.verify:
        if not os.path.exists(SNAP):
            sys.exit("no snapshot — run --snapshot BEFORE the night's update")
        sys.exit(verify(json.load(open(SNAP))))
    else:
        ap.print_help()
