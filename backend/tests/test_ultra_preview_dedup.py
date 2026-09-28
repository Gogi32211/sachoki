"""ultra-preview's single-flight dedup (2026-09-23).

The Preview scan is measured at ~36s / all 10 cores (preview_scan.py's own docstring, via a
ProcessPoolExecutor). Diagnosed the same day: several near-simultaneous identical requests —
React StrictMode remounts, a reload while Preview mode is on, more than one tab on the same
universe — each spun up its OWN process pool with no coordination, and stacked up enough
concurrent CPU-bound work to leave the backend unresponsive to even trivial requests for tens
of seconds at a stretch (load average ~12-13 on a 10-core box).

The fix wraps the endpoint in scan_cache.cached (the SAME single-flight TTL memo already used
for the Edge board's identical StrictMode-remount problem) — a concurrent duplicate call BLOCKS
on the per-key lock and reuses the first call's result rather than starting its own scan. These
tests exercise scan_cache directly against a fake "expensive scan" (a counter + a short sleep,
standing in for the real 36s ProcessPoolExecutor work), which is the same primitive the actual
fix is built on, without needing to run the real 10-core scan or hit the network.
"""
from __future__ import annotations
import os, sys, threading, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import scan_cache


def _fresh_module():
    """A clean scan_cache-shaped instance per test, so tests can't see each other's keys."""
    import importlib
    import types
    mod = importlib.reload(scan_cache)
    return mod


def test_concurrent_identical_calls_run_the_expensive_work_only_once():
    # Single-flight means only the ONE thread that wins the lock ever calls expensive() at all —
    # the other 4 block INSIDE cached(), on the lock, never reaching this function. So the overlap
    # to prove is "4 callers start before the 1st has returned", not "5 threads inside expensive()".
    sc = _fresh_module()
    calls = []
    lock_acquired = threading.Event()

    def expensive():
        lock_acquired.set()
        calls.append(1)
        time.sleep(0.1)
        return {"scanned": 501}

    results = [None] * 5
    def worker(i):
        results[i] = sc.cached("ultra-preview:sp500:None:None:20", expensive, ttl=45)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    threads[0].start()
    lock_acquired.wait(timeout=2)          # the first caller is now inside expensive(), still sleeping
    for t in threads[1:]:
        t.start()                          # these 4 arrive WHILE the first is still running
    for t in threads: t.join(timeout=5)

    assert len(calls) == 1, f"expensive() ran {len(calls)} times for 5 concurrent identical calls"
    assert all(r == {"scanned": 501} for r in results), "every caller must get the SAME result"


def test_different_keys_do_not_block_each_other():
    """A sp500 preview and a split preview are genuinely different work — dedup must not
    serialise them behind one lock, only calls that share a key."""
    sc = _fresh_module()
    order = []
    lock_held = threading.Event()

    def slow_sp500():
        lock_held.set()
        time.sleep(0.2)
        order.append("sp500")
        return "sp500-result"

    def fast_split():
        lock_held.wait(timeout=2)   # start only once sp500's lock is held
        order.append("split")
        return "split-result"

    t1 = threading.Thread(target=lambda: sc.cached("ultra-preview:sp500", slow_sp500, ttl=45))
    t1.start()
    r2 = sc.cached("ultra-preview:split", fast_split, ttl=45)
    t1.join(timeout=2)

    assert r2 == "split-result"
    assert order == ["split", "sp500"], "the different-key call must not wait for sp500's lock"


def test_a_second_call_after_the_first_finishes_within_ttl_reuses_the_result():
    sc = _fresh_module()
    calls = []

    def expensive():
        calls.append(1)
        return {"n": len(calls)}

    r1 = sc.cached("ultra-preview:key", expensive, ttl=45)
    r2 = sc.cached("ultra-preview:key", expensive, ttl=45)   # sequential, still within TTL
    assert r1 == r2 == {"n": 1}
    assert len(calls) == 1


def test_the_first_caller_is_never_starved_by_a_flood_of_duplicates():
    """The stated alternative — cancel the in-flight scan and restart on every new trigger —
    risks never completing under repeated rapid triggers. Single-flight guarantees the FIRST
    request that starts the work always runs it to completion once, however many duplicates
    pile up behind it while it runs."""
    sc = _fresh_module()
    started = threading.Event()
    finished = threading.Event()

    def expensive():
        started.set()
        time.sleep(0.15)
        finished.set()
        return "done"

    t = threading.Thread(target=lambda: sc.cached("ultra-preview:flood", expensive, ttl=45))
    t.start()
    started.wait(timeout=1)
    # a flood of duplicate triggers arriving WHILE the first is still running
    for _ in range(20):
        threading.Thread(target=lambda: sc.cached("ultra-preview:flood", expensive, ttl=45)).start()
        time.sleep(0.005)
    t.join(timeout=2)
    assert finished.is_set(), "the first in-flight scan must run to completion despite a flood of duplicates"
