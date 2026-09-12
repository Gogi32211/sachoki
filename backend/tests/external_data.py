"""tests/external_data.py — the ONE gate for tests that need the canonical research dataset.

TEST CONTRACT, frozen 2026-09-12. There are exactly two classes, and no third "optional" one.

  HERMETIC       must run on any clean clone: no network, no DuckDB corpus, no
                 /Volumes/QUANT_RESEARCH. A missing dependency, an assertion mismatch or a
                 missing in-repo fixture is a FAIL. "The volume is not mounted" is not an
                 excuse in this class.
  EXTERNAL_DATA  carries @pytest.mark.external_data. Dataset present -> the test runs and a
                 failure is a real failure. Dataset absent -> SKIP, with a concrete reason.
                 Collection must succeed either way, so nothing here may run at import time.

⚠️ SKIP MEANS ABSENT AND NOTHING ELSE. A dataset that is present but broken — an unreadable
file, a missing `bars` table, the wrong schema, a query that will not run — is a FAIL. Skipping
those would rename corruption to "I have no data", which is the exact confusion this module
exists to end. `require_external_test_data` therefore checks only availability and returns; it
never wraps the caller's queries.

The availability question is answered through the APPLICATION'S OWN path contract —
studio.db.tf_db_path() and studio.mount_guard.require_for_path() — never a hardcoded
/Volumes/... test. Otherwise test infrastructure and app infrastructure drift into two different
truths about where the data is, which is how this whole class of confusion started.
"""
from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request
from contextlib import contextmanager

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_REASON = "external research dataset unavailable"
_SERVICE_REASON = "external service unavailable"


def dataset_path(tf: str = "1d") -> str:
    """Resolve the dataset the same way the application does. Importing this module must not
    touch the filesystem, so the resolution happens here, at call time."""
    from studio.db import tf_db_path
    return tf_db_path(tf)


def require_external_test_data(tf: str = "1d") -> str:
    """Return the dataset path, or skip if it is genuinely unavailable.

    Skips on exactly two conditions, both meaning "the canonical dataset is not here":
      * the canonical volume is not mounted (or a different volume is mounted in its place),
        as judged by mount_guard — not by a string comparison on a path;
      * the database file does not exist.

    Everything else is left to raise, because everything else is a real failure.
    """
    path = dataset_path(tf)

    from studio.mount_guard import ExternalVolumeUnavailable, require_for_path
    try:
        require_for_path(path, purpose=f"pytest external-data test ({tf})")
    except ExternalVolumeUnavailable as e:
        pytest.skip(f"{_REASON}: {e}")

    if not os.path.exists(path):
        pytest.skip(f"{_REASON}: {os.path.basename(path)} ({path})")
    return path


@contextmanager
def external_conn(tf: str = "1d", table: str = "bars"):
    """Read-only connection to the canonical dataset, with the present-but-broken case made
    loud. Absence already skipped above; from here on every problem is a failure, and the
    assertion below turns a silent schema drift into a message that names the cause."""
    import duckdb
    path = require_external_test_data(tf)
    con = duckdb.connect(path, read_only=True)
    try:
        found = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        assert table in found, (
            f"dataset present but broken: {path} has no `{table}` table (found {found[:12]}). "
            f"This is a FAILURE, not a skip — the data is there and it is wrong.")
        yield con
    finally:
        con.close()


def dataset_available(tf: str = "1d") -> bool:
    """Non-skipping probe, for reporting only. Never use this to decide whether to assert."""
    try:
        path = dataset_path(tf)
        from studio.mount_guard import ExternalVolumeUnavailable, require_for_path
        try:
            require_for_path(path, purpose="probe")
        except ExternalVolumeUnavailable:
            return False
        return os.path.exists(path)
    except Exception:
        return False


def require_external_artifact(path: str, what: str | None = None) -> str:
    """A built artifact that is not in the repo — a parquet, a cache, an export.

    Absent -> skip. Present -> return, and every problem after that (unreadable, wrong schema,
    missing columns) is the caller's FAILURE. Do not wrap the caller's read in a try/skip.
    """
    if not os.path.exists(path):
        pytest.skip(f"{_REASON}: {what or os.path.basename(path)} ({path})")
    return path


def require_local_service(base: str, health: str = "/api/health", timeout: float = 3.0) -> str:
    """A locally running service — the backend on :8080.

    NOT REACHABLE -> skip: nothing is listening, so there is nothing to test.
    REACHABLE BUT WRONG -> fail: an HTTP error or a non-200 means the service IS there and is
    answering incorrectly, which is a defect, not an absent dependency.

    Called at test runtime, never in a skipif decorator: a decorator argument is evaluated at
    import, which would put a network call inside collection and break the rule that collection
    works with no network.
    """
    try:
        with urllib.request.urlopen(f"{base}{health}", timeout=timeout) as r:
            status = r.status
    except urllib.error.HTTPError as e:                      # it answered — badly
        pytest.fail(f"service at {base} answered {health} with HTTP {e.code}: present but broken")
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        pytest.skip(f"{_SERVICE_REASON}: {base} ({type(e).__name__}: {e})")
    assert status == 200, (f"service at {base} answered {health} with {status} — present but "
                           f"broken, so this is a FAILURE and not a skip")
    return base


def service_available(base: str, health: str = "/api/health", timeout: float = 3.0) -> bool:
    """Non-skipping probe, for reporting only."""
    try:
        with urllib.request.urlopen(f"{base}{health}", timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False
