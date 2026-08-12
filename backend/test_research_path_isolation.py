"""A static scan of the research path, independent of anything running.

The runtime guard is armed while an execution is open. That covers the read that happens during
a study and says nothing about a module that opens a connection at import time, caches a
DataFrame at module scope, or reads during a request that never opened an execution. Those never
meet the guard, so a second line is needed that does not depend on execution timing at all.

WHAT THIS IS AND IS NOT. It is a scan of a DECLARED list of modules — the ones on the research
execution path — for the direct constructors and for hard-coded paths into protected sources.
It is not a proof about the whole codebase, and it never will be: this application has many
modules that read these databases for reasons that are not research, and they are entitled to.

So the scope is explicit and the failure mode is a maintenance one: a new research module has to
be added to `RESEARCH_MODULES` to be covered. That is a weakness worth stating rather than
hiding behind a green tick, and it is why the guarantee is called ENFORCED_IN_PROCESS and not
ISOLATED. Under isolation this file would be unnecessary, because the research worker would hold
no path to open.
"""
from __future__ import annotations

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# The research execution path. Every module here runs while a study is open, or decides what a
# study is permitted to see.
RESEARCH_MODULES = [
    "studio_session_api.py",
    "research_session.py",
    "research_family.py",
    "evidence_boundary.py",
    "studio_semantics_api.py",
]

# The gateway and the access layer are the implementation of the barrier; they contain the
# constructors by necessity, which is the point of concentrating them in two files.
IMPLEMENTATION = {"data_gateway.py", "data_access.py"}

FORBIDDEN = [
    (re.compile(r"\bduckdb\.connect\s*\("), "duckdb.connect"),
    (re.compile(r"\bread_parquet\s*\("), "read_parquet"),
    (re.compile(r"\bpd\.read_csv\s*\(\s*[\"'][^\"']*(bars|opportunit)"), "read_csv on a source"),
    (re.compile(r"[\"'][^\"']*studio_analytics\.duckdb[\"']"), "a hard-coded database path"),
    (re.compile(r"[\"'][^\"']*opportunities[^\"']*\.parquet[\"']"), "a hard-coded artifact path"),
]

# studio_session_api registers the source with the gateway, which is the one legitimate place a
# path and a reader appear together outside the implementation modules. The allowance is narrow
# and named, so it cannot quietly grow.
ALLOWED = {
    ("studio_session_api.py", "duckdb.connect"),        # inside _bars_reader, the gateway's own
    ("studio_session_api.py", "a hard-coded database path"),
}

ok = fail = 0


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                           # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def t1_no_research_module_reaches_a_source_directly():
    """the scan itself"""
    offenders = []
    for name in RESEARCH_MODULES:
        path = os.path.join(HERE, name)
        if not os.path.exists(path):
            offenders.append((name, "MISSING — the declared list is stale"))
            continue
        with open(path) as f:
            for lineno, line in enumerate(f, 1):
                if line.lstrip().startswith("#"):
                    continue
                for pattern, label in FORBIDDEN:
                    if pattern.search(line) and (name, label) not in ALLOWED:
                        offenders.append((name, lineno, label, line.strip()[:70]))
    assert not offenders, (
        f"research modules reach data outside the gateway: {offenders}. Every read on this path "
        f"must go through ExecutionContext, or it produces no footprint and the completeness "
        f"attestation becomes a statement about instrumentation rather than about reads.")


def t2_the_allowance_is_exercised_and_narrow():
    """an allowance nobody uses is dead; an allowance nobody checks is a hole"""
    with open(os.path.join(HERE, "studio_session_api.py")) as f:
        src = f.read()
    assert "_bars_reader" in src, "the allowance names a reader that no longer exists"
    assert src.count("duckdb.connect") == 1, (
        f"the allowance is for ONE call inside the gateway's reader; there are "
        f"{src.count('duckdb.connect')}")
    assert "GW.REGISTRY.register" in src, \
        "the path appears but the source is never registered with the gateway"


def t3_the_application_actually_registers_its_sources():
    """a registry nothing populates protects nothing, however correct the guard is

    This imports the application module on purpose. The registration lives there, next to the
    reader, and a scan that only inspected `data_gateway` would report a healthy barrier around
    an empty set of sources.
    """
    import studio_session_api                                        # noqa: F401, PLC0415
    import data_gateway as GW                                        # noqa: PLC0415
    assert "bars_1d" in GW.REGISTRY.ids(), \
        f"the protected registry holds {GW.REGISTRY.ids()}; the bars database is not defended"
    reg = GW.REGISTRY.get("bars_1d")
    assert GW.REGISTRY.protects(reg.path) is not None, \
        "the source is registered by id but its path is not guarded, so the direct route is open"
    for name in IMPLEMENTATION:
        assert os.path.exists(os.path.join(HERE, name)), name


def t4_the_guarantee_is_not_overstated_anywhere():
    """no module may claim ISOLATED while the barrier is a patched function"""
    import data_gateway as GW                                        # noqa: PLC0415
    assert GW.GUARANTEE == GW.ENFORCED_IN_PROCESS
    for name in RESEARCH_MODULES + sorted(IMPLEMENTATION):
        path = os.path.join(HERE, name)
        if not os.path.exists(path):
            continue
        with open(path) as f:
            src = f.read()
        assert "GUARANTEE = ISOLATED" not in src, f"{name} claims isolation it does not have"


def t5_the_scan_notices_a_new_direct_read():
    """the guard shown its defect: a module that reads directly must fail t1"""
    probe = "\n".join([
        "import duckdb",
        "con = duckdb.connect('/tmp/whatever/studio_analytics.duckdb', read_only=True)",
    ])
    hits = [label for pattern, label in FORBIDDEN for line in probe.splitlines()
            if pattern.search(line)]
    assert "duckdb.connect" in hits and "a hard-coded database path" in hits, (
        f"the reproduction failed to reproduce: a module that opens the database directly was "
        f"supposed to trip the scan, and it produced {hits}")


print("=" * 100, flush=True)
print("  RESEARCH PATH ISOLATION — static scan, no runtime required", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_no_research_module_reaches_a_source_directly,
                        t2_the_allowance_is_exercised_and_narrow,
                        t3_the_application_actually_registers_its_sources,
                        t4_the_guarantee_is_not_overstated_anywhere,
                        t5_the_scan_notices_a_new_direct_read], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
