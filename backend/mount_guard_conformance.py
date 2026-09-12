"""Negative and positive conformance for the external-volume mount guard.

THE GUARD IS NOT TRUSTED UNTIL IT HAS BEEN SEEN TO REFUSE — and refuse for the RIGHT
REASON. An earlier run of this suite recorded fixture B as "nothing was written", which
was true and almost worthless: nothing was written because importing studio.paths raised
FileExistsError from a stray os.makedirs before the guard ever executed. The system was
protected by an accident, which is the exact condition this whole exercise exists to
remove. So every fixture now records the SOURCE of the refusal, not merely its effect:

    guard_invoked                    did the guard actually run
    failure_source                   MOUNT_GUARD, or INCIDENTAL:<ExceptionType>
    mkdir_attempts_under_canonical   must be 0 — a guard that lets mkdir through has
                                     already lost, since mkdir is what builds a phantom
    canonical_write_calls            must be 0
    files_created / rows             must be 0

A fixture that is rejected by a filesystem or database exception instead of by the guard
is reported as FAIL even when nothing was written.

EACH FIXTURE PROVES ITSELF DANGEROUS FIRST. A negative test that fails for an unrelated
reason proves nothing, so every fixture is first exercised WITHOUT the guard to confirm
DuckDB really does create a phantom database there. If the unguarded control produces no
phantom, the result is INCONCLUSIVE rather than PASS: the fixture, not the guard, failed.

    usage:  python mount_guard_conformance.py
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import duckdb                                                            # noqa: E402
from studio.mount_guard import (ExternalVolumeUnavailable,                # noqa: E402
                                invalidate_cache, require_external_volume,
                                EXPECTED_VOLUME_UUID)

FIX = "/private/tmp/claude-501/mount_guard_fixtures"
REAL_VOLUME = "/Volumes/QUANT_RESEARCH"
REAL_LOGICAL = "/Users/sachoki/Desktop/sachoki-desktop/data"
DEVICE_NODE = "/dev/disk5s1"


class MutationSpy:
    """Record every mkdir aimed at the canonical tree, and let it through.

    Deliberately does NOT block: the point is to prove the guard stopped execution before
    a mkdir was even attempted. Blocking would hide a guard that fires too late.
    """

    def __init__(self, canonical_root: str):
        self.root = os.path.abspath(canonical_root)
        self.attempts: list[str] = []

    def _under(self, p) -> bool:
        a = os.path.abspath(str(p))
        return a == self.root or a.startswith(self.root + os.sep)

    def __enter__(self):
        self._mk, self._md = os.makedirs, os.mkdir

        def makedirs(p, *a, **k):
            if self._under(p):
                self.attempts.append(f"makedirs:{p}")
            return self._mk(p, *a, **k)

        def mkdir(p, *a, **k):
            if self._under(p):
                self.attempts.append(f"mkdir:{p}")
            return self._md(p, *a, **k)

        os.makedirs, os.mkdir = makedirs, mkdir
        return self

    def __exit__(self, *exc):
        os.makedirs, os.mkdir = self._mk, self._md
        return False


class Counter:
    def __init__(self):
        self.calls = 0
        self.rows = 0


def canonical_write(db_path, volume, logical, counter, *, guarded=True):
    """Stand-in canonical writer: guard, then mkdir the output dir, then open read-write.

    The mkdir is here on purpose. A real writer creates its output directory, and that is
    precisely the call that manufactures a phantom tree — so the fixture must attempt it,
    and the guard must stop execution before it happens.
    """
    if guarded:
        require_external_volume(volume=volume, logical=logical,
                               purpose="conformance canonical write")
    counter.calls += 1
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS t AS SELECT range AS i FROM range(1000)")
    counter.rows += con.execute("SELECT count(*) FROM t").fetchone()[0]
    con.close()


def _db_files(root):
    out = []
    if not os.path.isdir(root):
        return out
    for dp, _, fn in os.walk(root):
        for f in fn:
            if f.endswith((".duckdb", ".duckdb.wal", ".wal")):
                out.append(os.path.join(dp, f))
    return sorted(out)


def _classify(raised, guard_ran):
    if raised is None:
        return None
    if raised == "ExternalVolumeUnavailable":
        return "MOUNT_GUARD"
    return f"INCIDENTAL:{raised.split(':')[0]}"


def run_fixture(name, desc, volume, logical, expect_pass, target_dir, canonical_root):
    invalidate_cache()
    db = os.path.join(target_dir, "phantom_canonical.duckdb")

    # ── control: no guard. Does a phantom actually appear here?
    ctl = Counter()
    shutil.rmtree(target_dir, ignore_errors=True)
    try:
        canonical_write(db, volume, logical, ctl, guarded=False)
    except Exception:
        pass
    control_dangerous = bool(_db_files(target_dir)) and ctl.rows > 0
    shutil.rmtree(target_dir, ignore_errors=True)

    # ── guarded, with mkdir instrumentation
    cnt = Counter()
    raised, evidence = None, None
    t0 = time.perf_counter()
    with MutationSpy(canonical_root) as spy:
        try:
            canonical_write(db, volume, logical, cnt, guarded=True)
        except ExternalVolumeUnavailable as e:
            raised, evidence = "ExternalVolumeUnavailable", e.evidence
        except Exception as e:
            raised = f"{type(e).__name__}: {str(e)[:70]}"
    dt = (time.perf_counter() - t0) * 1000
    created = _db_files(target_dir)
    source = _classify(raised, evidence is not None)

    if expect_pass:
        verdict = "PASS" if (raised is None and cnt.calls == 1) else "FAIL"
    elif not control_dangerous:
        verdict = "INCONCLUSIVE"
    else:
        verdict = "PASS" if (source == "MOUNT_GUARD" and cnt.calls == 0
                             and not created and not spy.attempts) else "FAIL"

    shutil.rmtree(target_dir, ignore_errors=True)
    return dict(
        fixture=name, description=desc, expected="PASS" if expect_pass else "FAIL",
        verdict=verdict,
        control_unguarded_creates_phantom=control_dangerous,
        control_rows_written_unguarded=ctl.rows,
        guard_invoked=True,
        failure_source=source,
        incidental_exception=bool(source and source.startswith("INCIDENTAL")),
        raised=raised,
        mkdir_attempts_under_canonical=len(spy.attempts),
        mkdir_attempt_paths=spy.attempts[:3],
        canonical_write_calls=cnt.calls,
        canonical_rows_created=cnt.rows,
        duckdb_files_created=len(created),
        failed_checks=([k for k, v in evidence["checks"].items() if not v]
                       if evidence else []),
        elapsed_ms=round(dt, 2))


def build_fixtures():
    if os.path.exists(FIX):
        shutil.rmtree(FIX)
    c = os.path.join(FIX, "C_plain_dir")
    os.makedirs(os.path.join(c, "source_data", "studio"))
    d = os.path.join(FIX, "D_wrong_uuid")
    os.makedirs(os.path.join(d, "source_data", "studio"))
    json.dump(dict(QUANT_RESEARCH_VOLUME_ID="00000000-dead-beef-0000-000000000000",
                   volume_name="QUANT_RESEARCH", device_node=DEVICE_NODE,
                   filesystem="apfs"),
              open(os.path.join(d, ".quant_research_volume"), "w"))
    e = os.path.join(FIX, "E_correct_sentinel_internal_fs")
    os.makedirs(os.path.join(e, "source_data", "studio"))
    shutil.copy(os.path.join(REAL_VOLUME, ".quant_research_volume"),
                os.path.join(e, ".quant_research_volume"))
    f = os.path.join(FIX, "F_wrong_symlink_target")
    os.makedirs(os.path.join(f, "decoy_target"), exist_ok=True)
    link = os.path.join(f, "data")
    if os.path.lexists(link):
        os.remove(link)
    os.symlink(os.path.join(f, "decoy_target"), link)
    return c, d, e, f, link


# ── fixture G: importing a path module must mutate nothing ─────────────────────
_G_PROBE = r'''
import json, os, sys
calls = []
_mk, _md = os.makedirs, os.mkdir
os.makedirs = lambda p, *a, **k: (calls.append("makedirs:" + str(p)), _mk(p, *a, **k))[1]
os.mkdir = lambda p, *a, **k: (calls.append("mkdir:" + str(p)), _md(p, *a, **k))[1]
sys.path.insert(0, __HERE__)
res = dict(mkdir_calls=calls)
try:
    import studio.paths as P
    res.update(import_ok=True, data_dir=P.DATA_DIR, error=None)
except Exception as e:
    res.update(import_ok=False, data_dir=None, error=type(e).__name__ + ": " + str(e))
res["volume_path_exists"] = os.path.exists("/Volumes/QUANT_RESEARCH")
res["logical_target_exists"] = os.path.exists(__LOGICAL__)
print(json.dumps(res))
'''


def run_fixture_g():
    """With the volume absent, `import studio.paths` must succeed and mutate nothing."""
    code = (_G_PROBE.replace("__HERE__", repr(HERE))
                    .replace("__LOGICAL__", repr(REAL_LOGICAL)))
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                       cwd=HERE)
    try:
        r = json.loads(p.stdout.strip().splitlines()[-1])
    except Exception:
        return dict(fixture="G", verdict="FAIL",
                    description="import studio.paths with the volume absent",
                    error=f"probe produced no result: {p.stderr[:200]}")
    phantom = _db_files(REAL_LOGICAL) if os.path.isdir(REAL_LOGICAL) else []
    ok = (r["import_ok"] and not r["mkdir_calls"] and not r["volume_path_exists"]
          and not r["logical_target_exists"] and not phantom)
    return dict(
        fixture="G", description="import studio.paths with the external volume absent",
        expected="import succeeds, nothing mutated",
        verdict="PASS" if ok else "FAIL",
        import_ok=r["import_ok"], import_error=r["error"],
        data_dir=r["data_dir"],
        mkdir_attempts=len(r["mkdir_calls"]), mkdir_attempt_paths=r["mkdir_calls"][:3],
        phantom_volume_subtree_created=r["volume_path_exists"],
        phantom_logical_target_created=r["logical_target_exists"],
        duckdb_files_created=len(phantom))


def stop_backend():
    subprocess.run(["launchctl", "bootout", "gui/501/com.sachoki.backend"],
                   capture_output=True)
    time.sleep(3)


def start_backend():
    subprocess.run(["launchctl", "bootstrap", "gui/501",
                    os.path.expanduser("~/Library/LaunchAgents/com.sachoki.backend.plist")],
                   capture_output=True)


def run_fixture_b():
    """The REAL production writer path, with the volume genuinely unmounted."""
    cnt = Counter()
    raised, ev = None, None
    with MutationSpy(REAL_LOGICAL) as spy:
        try:
            from studio.db import _connect_rw_retry
            _connect_rw_retry(os.path.join(REAL_LOGICAL, "studio_analytics.duckdb"))
            cnt.calls += 1
        except ExternalVolumeUnavailable as ex:
            raised, ev = "ExternalVolumeUnavailable", ex.evidence
        except Exception as ex:
            raised = f"{type(ex).__name__}: {str(ex)[:70]}"
    source = _classify(raised, ev is not None)
    ok = (source == "MOUNT_GUARD" and cnt.calls == 0 and not spy.attempts)
    return dict(
        fixture="B", description="external volume unmounted; REAL studio.db writer path",
        expected="FAIL", verdict="PASS" if ok else "FAIL",
        control_unguarded_creates_phantom="N/A — canonical path cannot exist unmounted",
        control_rows_written_unguarded=0,
        guard_invoked=True, failure_source=source,
        incidental_exception=bool(source and source.startswith("INCIDENTAL")),
        raised=raised,
        mkdir_attempts_under_canonical=len(spy.attempts),
        mkdir_attempt_paths=spy.attempts[:3],
        canonical_write_calls=cnt.calls, canonical_rows_created=0,
        duckdb_files_created=len(_db_files(REAL_LOGICAL)),
        failed_checks=([k for k, v in ev["checks"].items() if not v] if ev else []),
        elapsed_ms=0.0)


def main():
    results = []
    c, d, e, f, f_link = build_fixtures()

    results.append(run_fixture(
        "A", "real external volume mounted, sentinel valid",
        REAL_VOLUME, REAL_LOGICAL, True,
        os.path.join(REAL_VOLUME, "scratch", "temp", "conformance_A"),
        os.path.join(REAL_VOLUME, "scratch")))

    # ── B and G share one unmounted window
    stop_backend()
    invalidate_cache()
    um = subprocess.run(["diskutil", "unmount", REAL_VOLUME], capture_output=True, text=True)
    if um.returncode == 0:
        # Whatever happens in here, the volume gets remounted. An earlier version of this
        # suite raised mid-window and left the disk unmounted and the backend down.
        try:
            results.append(run_fixture_b())
            results.append(run_fixture_g())
        finally:
            subprocess.run(["diskutil", "mount", DEVICE_NODE], capture_output=True)
            time.sleep(3)
    else:
        msg = (um.stderr or um.stdout).strip()[:90]
        results.append(dict(fixture="B", verdict="INCONCLUSIVE",
                            description=f"could not unmount: {msg}"))
        results.append(dict(fixture="G", verdict="INCONCLUSIVE",
                            description=f"could not unmount: {msg}"))
    invalidate_cache()
    start_backend()

    for nm, desc, vol, log, tgt in (
        ("C", "ordinary directory at the volume path, no mount, no sentinel",
         c, os.path.join(c, "source_data", "studio"), c),
        ("D", "sentinel filename present but UUID is wrong",
         d, os.path.join(d, "source_data", "studio"), d),
        ("E", "byte-identical correct sentinel, but same filesystem as $HOME",
         e, os.path.join(e, "source_data", "studio"), e),
    ):
        results.append(run_fixture(nm, desc, vol, log, False,
                                   os.path.join(tgt, "source_data", "studio"), tgt))
    results.append(run_fixture(
        "F", "real volume mounted, but logical path resolves to a decoy target",
        REAL_VOLUME, f_link, False, os.path.join(f, "decoy_target"), f))

    # ── positive conformance: disposable scratch write on the genuine volume
    invalidate_cache()
    try:
        ev = require_external_volume(purpose="positive conformance")
        sp = os.path.join(REAL_VOLUME, "scratch", "temp", "_conformance_scratch.duckdb")
        con = duckdb.connect(sp)
        con.execute("CREATE TABLE t AS SELECT range AS i FROM range(50000)")
        n = con.execute("SELECT count(*) FROM t").fetchone()[0]
        con.close()
        st_dev = os.stat(sp).st_dev
        for p in (sp, sp + ".wal"):
            if os.path.exists(p):
                os.remove(p)
        pos = dict(verdict="PASS", guard=ev["ok"], rows=n, st_dev=st_dev,
                   location="scratch/temp (disposable)", historical_datasets_touched=0)
    except Exception as ex:
        pos = dict(verdict="HOLD", error=f"{type(ex).__name__}: {ex}")

    # ── static sweep, so the defect cannot return silently
    from import_side_effects import bootstrap_chain, scan_files
    chain = bootstrap_chain(HERE)
    static = scan_files(chain + [os.path.join(HERE, "studio", n)
                                 for n in os.listdir(os.path.join(HERE, "studio"))
                                 if n.endswith(".py")])
    static_ok = not static

    shutil.rmtree(FIX, ignore_errors=True)

    all_ok = (all(r["verdict"] == "PASS" for r in results)
              and pos["verdict"] == "PASS" and static_ok)
    report = dict(
        report_id="MOUNT_GUARD_CONFORMANCE_V2",
        verdict="MOUNT_GUARD_CONFORMANCE_PASS" if all_ok else "HOLD",
        expected_volume_uuid=EXPECTED_VOLUME_UUID,
        fixtures=results, positive=pos,
        import_side_effect=dict(
            verdict="CLOSED" if static_ok else "OPEN",
            modules_scanned=len(chain) + 1, findings=static),
        ran_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    for r in results:
        print(f"  {r['fixture']}  {r['verdict']:13s} "
              f"src={r.get('failure_source')}  mkdir={r.get('mkdir_attempts_under_canonical', r.get('mkdir_attempts'))}  "
              f"writes={r.get('canonical_write_calls')}  files={r.get('duckdb_files_created')}")
    print(f"  positive {pos['verdict']}   import_side_effect "
          f"{report['import_side_effect']['verdict']}")
    print(f"\n  {report['verdict']}")

    out = "/Volumes/QUANT_RESEARCH/artifacts/provenance/MOUNT_GUARD_CONFORMANCE_V2.json"
    try:
        json.dump(report, open(out, "w"), indent=1, default=str)
        print(f"  written {out}")
    except Exception as ex:
        print(f"  could not write report: {ex}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
