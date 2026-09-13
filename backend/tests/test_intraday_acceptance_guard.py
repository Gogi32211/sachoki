"""The acceptance baseline must never be silently overwritten.

WHY THIS EXISTS. `intraday_acceptance.py --snapshot` captures the PRE-run state of the 1H/4H stores;
`--verify` compares the post-run state against it. The baseline is the only link between the two
moments, so overwriting it after the run would leave --verify comparing a state against itself and
reporting PASS while proving nothing — a silent, total loss of the gate.

That risk was first handled by writing "do not re-snapshot" in three documents. It is now handled in
code, which is the lesson from the sibling defect: a verification artifact's creation must FAIL
CLOSED, never fall back. (The baseline itself was once written under `data/` — a symlink to an
external SSD where nothing is tracked — and a `git add … || …` fallback hid the fact that it had
never been committed at all.)

THE CONTRACT:
    baseline exists  →  --snapshot            REFUSE, non-zero exit, bytes untouched
                     →  --snapshot --force    overwrite allowed
                     →  --verify              read the existing baseline

Hermetic: temp files only. No DuckDB, no stores, no network — the guard runs BEFORE capture(), which
is itself the property being pinned.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys

import pytest

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

from intraday_acceptance import assert_baseline_writable          # noqa: E402


def _sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


def test_a_fresh_snapshot_is_allowed(tmp_path):
    assert_baseline_writable(str(tmp_path / "none.json"), force=False)   # must not raise


def test_a_second_snapshot_is_refused(tmp_path):
    p = tmp_path / "baseline.json"
    p.write_text(json.dumps({"tf": {}}))
    with pytest.raises(SystemExit) as e:
        assert_baseline_writable(str(p), force=False)
    assert "REFUSED" in str(e.value)
    assert "VOIDS THE ACCEPTANCE" in str(e.value), "the message must say what overwriting costs"


def test_the_refused_attempt_leaves_the_baseline_byte_identical(tmp_path):
    p = tmp_path / "baseline.json"
    p.write_text(json.dumps({"tf": {"1h": {"max_date": "2026-09-11"}}}))
    before = _sha(p)
    with pytest.raises(SystemExit):
        assert_baseline_writable(str(p), force=False)
    assert _sha(p) == before, "a refused snapshot must not touch the baseline"


def test_force_permits_a_new_cycle(tmp_path):
    p = tmp_path / "baseline.json"
    p.write_text("{}")
    assert_baseline_writable(str(p), force=True)                         # must not raise


def test_the_cli_exits_non_zero_and_reads_no_store(tmp_path):
    """End to end: the guard has to fire BEFORE capture() opens any database, so a refusal is
    instant and harmless even with the stores absent or unmounted."""
    p = tmp_path / "baseline.json"
    p.write_text(json.dumps({"tf": {}}))
    before = _sha(p)
    env = {**os.environ, "INTRADAY_ACCEPTANCE_SNAPSHOT": str(p)}
    r = subprocess.run([sys.executable, os.path.join(BACKEND, "intraday_acceptance.py"), "--snapshot"],
                       capture_output=True, text=True, env=env, cwd=BACKEND, timeout=120)
    assert r.returncode != 0, f"a refused snapshot must exit non-zero:\n{r.stdout}{r.stderr}"
    assert "REFUSED" in (r.stdout + r.stderr)
    assert _sha(p) == before
