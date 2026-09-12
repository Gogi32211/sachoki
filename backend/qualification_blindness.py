"""Source-selection blindness: make "do not choose the source that gives better results"
a thing the process CANNOT do, rather than a thing it promises not to.

THE PROBLEM WITH THE TIMESTAMP AUDIT. The qualification contract already says the canonical
PIT source must be selected and sealed before its membership table is joined to prices or
outcomes, and that the seal time must precede the first join. That is a good rule and a
weak mechanism: it detects the violation afterwards, in an artifact nobody re-reads, and
only if the violation took the form of a recorded join. A qualification script that reads
one price file to "sanity check coverage" leaves no trace at all.

So the rule is enforced where it can actually bite: at the read. During candidate
qualification, the process may open the fixture archive, the candidate's own membership
data, and identity/reference data. Every other data read raises.

ALLOWLIST, NOT DENYLIST. A denylist protects against the sensitive paths someone
remembered to list. An allowlist refuses everything not named, so a price store nobody
thought of is blocked by default and a new outcome artifact does not silently become
readable. Code and library reads are never intercepted — this governs DATA access, not
Python imports.

WHAT IT DOES NOT CLAIM. This is a guardrail against inadvertent contamination in our own
scripts, not a security sandbox. A determined caller can obviously bypass it; the point is
that "I only peeked at coverage" stops being possible by accident, and every attempt is
recorded whether it was allowed or refused.

    with blindness(allow=[...], audit="candidate_X"):
        ...run the frozen fixtures against a candidate...
"""
from __future__ import annotations

import builtins
import os
import time
from contextlib import contextmanager

DATA_EXT = (".duckdb", ".wal", ".parquet", ".npz", ".csv", ".feather", ".arrow", ".db",
            ".sqlite", ".h5", ".pkl")

# Roots that must never be read during qualification, listed for the audit's benefit.
# Enforcement does NOT depend on this list — the allowlist already refuses them.
SENSITIVE_ROOTS = ("/Users/sachoki/Desktop/sachoki-desktop/data",
                   "/Volumes/QUANT_RESEARCH/source_data")
SENSITIVE_PATTERNS = ("survivor", "episode", "opportunity", "edge_replay", "outcome",
                      "mfe", "historical_exposed", "capability_result")


class BlindnessViolation(RuntimeError):
    """A qualification process tried to read data it must not see."""

    def __init__(self, path, why, audit):
        self.path = path
        super().__init__(
            f"source-selection blindness: refused read of {path}\n"
            f"  reason  : {why}\n"
            f"  context : {audit}\n"
            f"  Candidate qualification may read the fixture archive, the candidate's own\n"
            f"  membership source, and identity/reference data. Price history, signals,\n"
            f"  episode counts and outcome artifacts are not readable here, so that the\n"
            f"  choice of source cannot be informed by what it yields.")


class _Recorder:
    def __init__(self, audit):
        self.audit = audit
        self.allowed: list[str] = []
        self.refused: list[dict] = []


_active: list[_Recorder] = []


def _is_data_path(p: str) -> bool:
    lp = p.lower()
    return lp.endswith(DATA_EXT)


def _why_sensitive(p: str) -> str:
    ap = os.path.abspath(p)
    for r in SENSITIVE_ROOTS:
        if ap == r or ap.startswith(r + os.sep):
            return f"path is under the canonical data root {r}"
    low = os.path.basename(ap).lower()
    for pat in SENSITIVE_PATTERNS:
        if pat in low:
            return f"filename matches the outcome-artifact pattern '{pat}'"
    return "path is not on the qualification allowlist"


def _check(path, allow_roots, rec: _Recorder):
    ap = os.path.abspath(str(path))
    for a in allow_roots:
        if ap == a or ap.startswith(a + os.sep):
            rec.allowed.append(ap)
            return
    why = _why_sensitive(ap)
    rec.refused.append(dict(path=ap, reason=why,
                            at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
    raise BlindnessViolation(ap, why, rec.audit)


@contextmanager
def blindness(allow, audit="candidate qualification"):
    """Restrict data reads to `allow` roots for the duration of the block.

    Yields the recorder, so a caller can seal the full access trail — allowed reads
    included — into the candidate's evaluation artifact.
    """
    allow_roots = [os.path.abspath(a) for a in allow]
    rec = _Recorder(audit)
    _active.append(rec)

    real_open = builtins.open
    patched = {}

    def guarded_open(file, mode="r", *a, **k):
        # only DATA reads are governed; writing and code/library reads are untouched
        if isinstance(file, (str, bytes, os.PathLike)):
            p = os.fsdecode(file) if not isinstance(file, str) else file
            if "r" in mode and "+" not in mode and _is_data_path(p):
                _check(p, allow_roots, rec)
        return real_open(file, mode, *a, **k)

    builtins.open = guarded_open

    try:
        import duckdb
        patched["duckdb"] = (duckdb, "connect", duckdb.connect)

        def guarded_connect(database=":memory:", *a, **k):
            if isinstance(database, str) and database != ":memory:":
                _check(database, allow_roots, rec)
            return patched["duckdb"][2](database, *a, **k)

        duckdb.connect = guarded_connect
    except ImportError:
        pass

    try:
        import pandas as pd
        for fn in ("read_parquet", "read_csv", "read_feather"):
            if hasattr(pd, fn):
                orig = getattr(pd, fn)
                patched[f"pd.{fn}"] = (pd, fn, orig)

                def mk(o):
                    def guarded(path, *a, **k):
                        if isinstance(path, (str, os.PathLike)):
                            _check(path, allow_roots, rec)
                        return o(path, *a, **k)
                    return guarded
                setattr(pd, fn, mk(orig))
    except ImportError:
        pass

    try:
        yield rec
    finally:
        builtins.open = real_open
        for mod, name, orig in patched.values():
            setattr(mod, name, orig)
        _active.pop()


# ── conformance ────────────────────────────────────────────────────────────────
def conformance(verbose=True):
    """Prove the guard refuses the reads it must, and permits the ones it must.

    Every negative case is first attempted WITHOUT the guard, because a refusal that would
    have failed anyway proves nothing about the guard.
    """
    import tempfile
    import duckdb
    import pandas as pd

    tmp = tempfile.mkdtemp(prefix="blindness_")
    allowed_root = os.path.join(tmp, "fixture_archive")
    forbidden_root = os.path.join(tmp, "data")
    os.makedirs(allowed_root); os.makedirs(forbidden_root)

    ok_pq = os.path.join(allowed_root, "candidate_membership.parquet")
    pd.DataFrame(dict(security_id=[1, 2], d=["2020-01-01", "2020-01-02"])).to_parquet(ok_pq)
    bad_pq = os.path.join(forbidden_root, "prices.parquet")
    pd.DataFrame(dict(close=[1.0, 2.0])).to_parquet(bad_pq)
    bad_db = os.path.join(forbidden_root, "studio_analytics.duckdb")
    c = duckdb.connect(bad_db); c.execute("create table bars as select 1 as i"); c.close()
    bad_out = os.path.join(forbidden_root, "T9_15M_SURVIVOR_STRUCTURE_V1.parquet")
    pd.DataFrame(dict(z=[1.0])).to_parquet(bad_out)

    checks = {}

    # controls: unguarded, all four reads must SUCCEED, or the fixtures prove nothing
    ctl = []
    for fn in (lambda: pd.read_parquet(ok_pq), lambda: pd.read_parquet(bad_pq),
               lambda: duckdb.connect(bad_db, read_only=True).close(),
               lambda: pd.read_parquet(bad_out)):
        try:
            fn(); ctl.append(True)
        except Exception:
            ctl.append(False)
    checks["control_all_reads_succeed_unguarded"] = all(ctl)

    with blindness(allow=[allowed_root], audit="conformance") as rec:
        try:
            pd.read_parquet(ok_pq)
            checks["allowlisted_parquet_permitted"] = True
        except Exception:
            checks["allowlisted_parquet_permitted"] = False

        for name, fn in (("price_parquet_refused", lambda: pd.read_parquet(bad_pq)),
                         ("price_duckdb_refused",
                          lambda: duckdb.connect(bad_db, read_only=True)),
                         ("outcome_artifact_refused", lambda: pd.read_parquet(bad_out))):
            try:
                fn()
                checks[name] = False
            except BlindnessViolation:
                checks[name] = True
            except Exception:
                checks[name] = False

        try:
            builtins.open(bad_db, "rb").close()
            checks["raw_open_of_db_refused"] = False
        except BlindnessViolation:
            checks["raw_open_of_db_refused"] = True
        except Exception:
            checks["raw_open_of_db_refused"] = False

        checks["refusals_recorded"] = len(rec.refused) == 4
        checks["allowed_reads_recorded"] = len(rec.allowed) >= 1

    # the patches must be fully removed on exit
    try:
        pd.read_parquet(bad_pq)
        checks["restored_after_block"] = True
    except Exception:
        checks["restored_after_block"] = False

    if verbose:
        for k, v in checks.items():
            print(f"    {'PASS' if v else 'FAIL'}  {k}")
    return dict(checks=checks, passed=all(checks.values()))


if __name__ == "__main__":
    r = conformance()
    print(f"  BLINDNESS CONFORMANCE {'PASS' if r['passed'] else 'FAIL'}")
    raise SystemExit(0 if r["passed"] else 1)
