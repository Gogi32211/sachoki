"""main.py must survive an optional router failing to import.

THE DEFECT (found 2026-09-13 while repairing the container build). Two module-level guards exist
precisely so the app can run without them:

    try:  from studio_api import router as studio_router
    except Exception as _studio_err:
        log.warning("Analytic Studio not available: %s", _studio_err)   # line 68

    try:  from qlib_lab.api import router as qlib_router
    except Exception as _qlib_err:
        log.warning("QLIB lab not available: %s", _qlib_err)            # line 74

...and `log = logging.getLogger(__name__)` was 35 lines further down, at line 103. So the handler
raised `NameError: name 'log' is not defined` and killed the process. The guard could not do the
one thing it was written for, and nothing noticed because on this host both imports succeed. It
surfaced only inside a container that was missing backend/studio — the import failed for real,
and the app died instead of logging and continuing.

An AST sweep of main.py's module level found exactly two such uses (lines 68 and 74) and no other
name read before it is assigned; the third early guard, around `dotenv`, only touches ImportError
and was always safe.

WHY A SUBPROCESS. The test has to import main with a dependency broken, and main is a large module
with a scheduler and route registration. Importing it inside the pytest process — twice, with
sys.modules surgery — would leak state into every later test. A subprocess gets a clean
interpreter, and `sys.modules[name] = None` makes any import of that name raise ImportError, which
is exactly the failure being guarded.

HERMETIC: no data, no DuckDB, no network. main imports fine without them (verified on a clean
clone), and a missing database is not what this is about.
"""
from __future__ import annotations

import os
import subprocess
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_PROBE = """
import sys
sys.modules[{name!r}] = None          # any import of it now raises ImportError
sys.path.insert(0, {backend!r})
import main
print("IMPORT_OK",
      "studio=%s" % getattr(main, "_STUDIO_AVAILABLE", "MISSING"),
      "qlib=%s" % getattr(main, "_QLIB_AVAILABLE", "MISSING"))
"""


def _import_main_without(name: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", _PROBE.format(name=name, backend=BACKEND)],
        capture_output=True, text=True, timeout=600, cwd=BACKEND)


@pytest.mark.parametrize("broken,flag", [("studio_api", "studio=False"),
                                         ("qlib_lab.api", "qlib=False")])
def test_main_imports_when_an_optional_router_is_unavailable(broken, flag):
    """The guard's whole purpose: warn, set the flag to False, and carry on."""
    r = _import_main_without(broken)
    assert "NameError" not in r.stderr, (
        f"the {broken} guard raised NameError instead of logging — the handler reads a name that "
        f"does not exist yet:\n{r.stderr[-1500:]}")
    assert r.returncode == 0, f"importing main without {broken} failed:\n{r.stderr[-1500:]}"
    assert "IMPORT_OK" in r.stdout, r.stdout + r.stderr[-800:]
    assert flag in r.stdout, f"expected {flag} in: {r.stdout.strip()}"


def test_the_logger_exists_before_the_first_guard_uses_it():
    """Pins the ordering directly, so the bug cannot come back by someone moving the logging
    block back down. Every module-level read of a name must come after its assignment."""
    import ast
    src = open(os.path.join(BACKEND, "main.py")).read()
    tree = ast.parse(src)

    assigned: dict[str, int] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    assigned.setdefault(t.id, node.lineno)

    early = []
    for node in tree.body:
        if isinstance(node, ast.Try):
            for handler in node.handlers:
                for n in ast.walk(handler):
                    if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load):
                        defined_at = assigned.get(n.id)
                        if defined_at and n.lineno < defined_at:
                            early.append((n.lineno, n.id, defined_at))

    assert not early, (
        "a module-level exception handler reads a name defined later — the handler will raise "
        f"NameError instead of handling anything: {sorted(set(early))}")
