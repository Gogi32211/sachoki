"""A static scan of the research path, discovered rather than listed.

The runtime guard is armed while an execution is open. That covers the read that happens during
a study and says nothing about a module that opens a connection at import time, caches a frame
at module scope, or reads during a request that never opened an execution. Those never meet the
guard, so a second line is needed that does not depend on execution timing.

WHY THE MANUAL LIST HAD TO GO. The first version named the modules to scan. It passed, and its
weakest point was the one thing about to change: the next phase adds files, and "remember to add
the new module to the scanner" is a precondition that fails silently and looks green while it
does. A guard whose coverage depends on someone remembering is a guard with a maintenance
bypass, and a maintenance bypass is still a bypass.

MEMBERSHIP IS A STRUCTURAL FACT, NOT A NAME. The first attempt at discovery matched filename
prefixes, and it was wrong twice over in one run: `data_*` swept up the application's own data
modules, and this codebase already contains `backend/studio/` — an existing product feature that
has nothing to do with Analytic Studio. Naming conventions collide with whatever the repository
already means by those words.

Nor could the scan simply cover everything and exempt the rest: 193 modules in this backend read
DuckDB or parquet directly, and they are entitled to. An exemption list of 193 entries is not a
policy, it is a formality.

So the namespace is derived from the IMPORT GRAPH, transitively closed:

    SEED           the research core — the session ledger, the family, the boundary, the
                   gateway, the transport surfaces
    NAMESPACE      every module that imports a seed, and every module that imports one of those,
                   to a fixed point
    INFRASTRUCTURE data_access.py · data_gateway.py — the barrier's own implementation, which
                   holds the raw constructors so that nothing else has to
    ALLOWED        one narrow, named exception per (file, finding), written down to exist

A new research module has to import the session or the gateway to participate in the accounting
at all, so it joins the namespace by doing its job rather than by being remembered. That does
not turn ENFORCED_IN_PROCESS into ISOLATED — under isolation this file would be unnecessary,
because the worker would hold no path to open — but it removes the maintenance bypass.

WHAT IT STILL DOES NOT COVER, stated rather than implied: a read through some other library, a
subprocess, or a module that runs research while importing none of the core. That last one is
not silent, though — a study whose reads never reach the gateway produces no footprint and no
receipt, and the confirmatory path answers UNKNOWN. Weak coverage here degrades to the safe
verdict rather than to a false CLEAN.
"""
from __future__ import annotations

import os
import re
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# The research core. Anything that reaches the accounting reaches it through one of these.
SEEDS = {
    "research_session", "research_family", "research_store", "evidence_boundary",
    "data_gateway", "studio_session_api", "studio_semantics_api",
}

# The barrier's own implementation. It holds the raw constructors so that nothing else has to.
INFRASTRUCTURE = {"data_access.py", "data_gateway.py"}

SKIP_DIRS = {".venv", "node_modules", "__pycache__", "research_ledger", ".git", "static"}

# Composition roots. `main.py` mounts every router in the application, including the research
# ones, so the import graph runs straight through it into the entire backend — build scripts,
# the scanner, the intraday builders. Being the file that WIRES research up is not the same as
# being research, and without this the closure floods and the scan becomes a list of everything.
#
# The set is declared rather than inferred, which is a manual element and therefore worth
# stating: adding a root removes it and everything reachable only through it from the scan. One
# entry, with a reason, is a policy; a growing list would be the maintenance bypass coming back.
COMPOSITION_ROOTS = {"main"}

# Tests are not the research path; they build fixtures and reproduce defects on purpose.
def _is_test(name: str) -> bool:
    return name.startswith("test_")


FORBIDDEN = [
    (re.compile(r"\bduckdb\.connect\s*\("), "duckdb.connect"),
    (re.compile(r"\bread_parquet\s*\("), "read_parquet"),
    (re.compile(r"\bread_csv\s*\(\s*[\"'][^\"']*(bars|opportunit)"), "read_csv on a source"),
    (re.compile(r"\bsqlite3\.connect\s*\("), "sqlite3.connect"),
    (re.compile(r"[\"'][^\"']*studio_analytics\.duckdb[\"']"), "a hard-coded database path"),
    (re.compile(r"[\"'][^\"']*opportunities[^\"']*\.parquet[\"']"), "a hard-coded artifact path"),
]

# studio_session_api registers the source WITH the gateway, which is the one place a path and a
# reader legitimately appear together outside the infrastructure modules.
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


# ── discovery ───────────────────────────────────────────────────────────────
def _imports(path: str) -> set:
    """Module names this file imports. Parsed, never executed."""
    import ast
    try:
        with open(path, errors="replace") as f:
            tree = ast.parse(f.read(), filename=path)
    except SyntaxError:
        return set()
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    return names


def discover(root: str, seeds: set = SEEDS) -> list:
    """The research namespace, closed transitively over the import graph.

    A module joins by importing the core, and a module that imports THAT module joins too, to a
    fixed point. Nobody registers anything; participating in the accounting is what puts a file
    in scope.
    """
    files = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".")]
        for name in filenames:
            if name.endswith(".py") and not _is_test(name):
                files[os.path.join(dirpath, name)] = _imports(os.path.join(dirpath, name))

    member_mods = set(seeds)
    changed = True
    while changed:                       # fixed point: importers of members become members
        changed = False
        for path, imports in files.items():
            mod = os.path.splitext(os.path.basename(path))[0]
            if mod in member_mods or mod in COMPOSITION_ROOTS:
                continue
            if imports & member_mods:
                member_mods.add(mod)
                changed = True

    return sorted(p for p in files
                  if os.path.splitext(os.path.basename(p))[0] in member_mods
                  and os.path.basename(p) not in INFRASTRUCTURE)


def scan(root: str) -> list:
    """(file, line, finding, source) for every direct data reach in the namespace."""
    offenders = []
    for path in discover(root):
        name = os.path.basename(path)
        with open(path, errors="replace") as f:
            for lineno, line in enumerate(f, 1):
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    continue
                for pattern, label in FORBIDDEN:
                    if pattern.search(line) and (name, label) not in ALLOWED:
                        offenders.append((name, lineno, label, line.strip()[:70]))
    return offenders


# ── the tests ───────────────────────────────────────────────────────────────
def t1_no_research_module_reaches_a_source_directly():
    """the scan itself, over everything it discovers"""
    offenders = scan(HERE)
    assert not offenders, (
        f"research modules reach data outside the gateway: {offenders}. Every read on this path "
        f"must go through ExecutionContext, or it produces no footprint and the completeness "
        f"attestation becomes a statement about instrumentation rather than about reads.")


def t2_a_new_module_is_covered_without_being_registered_anywhere():
    """the maintenance bypass, closed: membership is a property of the file

    Written against a temporary tree so it proves auto-coverage without adding a bad module to
    this repository. The old version of this file listed its targets by hand, so a file exactly
    like the one below would have been invisible until someone remembered it.
    """
    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, "nested", "deep"), exist_ok=True)
        # a new study, named nothing in particular, that uses the session ledger
        with open(os.path.join(tmp, "my_new_study.py"), "w") as f:
            f.write("import duckdb\nfrom research_session import ResearchSession\n"
                    "con = duckdb.connect('/data/studio_analytics.duckdb')\n")
        # and a helper of THAT, two hops from any seed and in a subdirectory
        with open(os.path.join(tmp, "nested", "deep", "loader.py"), "w") as f:
            f.write("import pandas as pd\nimport my_new_study\n"
                    "df = pd.read_parquet('/data/opportunities.parquet')\n")
        # a neighbour that touches none of it must stay OUT of scope
        with open(os.path.join(tmp, "unrelated_report.py"), "w") as f:
            f.write("import duckdb\ncon = duckdb.connect('/data/other.duckdb')\n")

        found = {os.path.basename(p) for p in discover(tmp)}
        assert {"my_new_study.py", "loader.py"} <= found, found
        assert "unrelated_report.py" not in found, (
            "a module unrelated to the research core was pulled in; the namespace has to stay "
            "narrow or the exemption list becomes the policy")

        offenders = {(o[0], o[2]) for o in scan(tmp)}
        assert ("my_new_study.py", "duckdb.connect") in offenders, offenders
        assert ("loader.py", "read_parquet") in offenders, (
            "a module two hops from the core was not scanned; the transitive closure is the "
            "whole reason this replaced a hand-written list")


def t3_discovery_actually_finds_the_known_research_path():
    """a scanner that discovers nothing also reports no offenders"""
    found = {os.path.basename(p) for p in discover(HERE)}
    assert "main.py" not in found, \
        "the composition root joined the namespace and the closure flooded the whole backend"
    assert len(found) < 30, (
        f"the namespace grew to {len(found)} modules. Either research spread, or the closure is "
        f"leaking through a new bridge the way it did through main.py: {sorted(found)}")
    for expected in ("studio_session_api.py", "research_session.py", "research_family.py",
                     "evidence_boundary.py", "studio_semantics_api.py"):
        assert expected in found, f"{expected} is on the research path and was not discovered"
    assert not (found & INFRASTRUCTURE), \
        "the barrier's own implementation was scanned as if it were research code"
    assert not any(_is_test(n) for n in found), "test fixtures were scanned as research code"


def t4_the_allowance_is_exercised_and_narrow():
    """an allowance nobody uses is dead; an allowance nobody checks is a hole"""
    with open(os.path.join(HERE, "studio_session_api.py")) as f:
        src = f.read()
    assert "_bars_reader" in src, "the allowance names a reader that no longer exists"
    assert src.count("duckdb.connect") == 1, (
        f"the allowance is for ONE call inside the gateway's reader; there are "
        f"{src.count('duckdb.connect')}")
    assert "GW.REGISTRY.register" in src, \
        "the path appears but the source is never registered with the gateway"
    for entry in ALLOWED:
        assert os.path.exists(os.path.join(HERE, entry[0])), \
            f"the allowance names {entry[0]}, which does not exist — a stale exemption"


def t5_the_application_actually_registers_its_sources():
    """a registry nothing populates protects nothing, however correct the guard is"""
    import studio_session_api                                        # noqa: F401, PLC0415
    import data_gateway as GW                                        # noqa: PLC0415
    assert "bars_1d" in GW.REGISTRY.ids(), \
        f"the protected registry holds {GW.REGISTRY.ids()}; the bars database is not defended"
    reg = GW.REGISTRY.get("bars_1d")
    assert GW.REGISTRY.protects(reg.path) is not None, \
        "the source is registered by id but its path is not guarded, so the direct route is open"


def t6_the_guarantee_is_not_overstated_anywhere():
    """no module may claim ISOLATED while the barrier is a patched function"""
    import data_gateway as GW                                        # noqa: PLC0415
    assert GW.GUARANTEE == GW.ENFORCED_IN_PROCESS
    for path in discover(HERE) + [os.path.join(HERE, n) for n in INFRASTRUCTURE]:
        with open(path, errors="replace") as f:
            src = f.read()
        assert "GUARANTEE = ISOLATED" not in src, \
            f"{os.path.basename(path)} claims isolation it does not have"


print("=" * 100, flush=True)
print("  RESEARCH PATH ISOLATION — discovered, not listed", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_no_research_module_reaches_a_source_directly,
                        t2_a_new_module_is_covered_without_being_registered_anywhere,
                        t3_discovery_actually_finds_the_known_research_path,
                        t4_the_allowance_is_exercised_and_narrow,
                        t5_the_application_actually_registers_its_sources,
                        t6_the_guarantee_is_not_overstated_anywhere], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
if not fail:
    print(f"\n  namespace: {len(discover(HERE))} modules discovered, "
          f"{len(INFRASTRUCTURE)} infrastructure, {len(ALLOWED)} named allowances", flush=True)
sys.exit(1 if fail else 0)
