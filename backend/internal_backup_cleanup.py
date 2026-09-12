"""STUDIO_INTERNAL_BACKUP_CLEANUP_V1 — the one authorised destructive cleanup.

Deletes exactly one directory: the verified internal pre-migration backup. Nothing else is
touched, and every gate below is fail-closed — uncertainty produces HOLD, never PASS.

WHY RENAME BEFORE DELETE. A byte-verified external copy proves the CONTENT is safe. It says
nothing about whether some code path still resolves the old internal PATH. An atomic rename
on the same filesystem is instant, reversible, and makes exactly that failure observable
while all 84 GB are still recoverable:

    verified backup -> atomic rename -> does everything still work? -> physical delete
                                     -> no: rename back, HOLD

THE DESTRUCTIVE TARGET IS ALWAYS A LITERAL PATH. No globs, no wildcards, no find -delete, no
parent cleanup, no pattern matching. Before the rmtree the path is asserted to be a real
directory, on the internal filesystem, not a symlink, not a mount point, not under /Volumes,
strictly inside the project directory but not the project directory itself, and not `data`.

WHAT SURVIVES. The external rollback archive is NOT authorised for deletion here, and is a
ROLLBACK ARCHIVE rather than a disaster-recovery backup: it shares a physical device with
the canonical store, so it covers logical failures and not the loss of that SSD.
"""
from __future__ import annotations
import hashlib, json, os, shutil, subprocess, sys, time                   # noqa: E402
from concurrent.futures import ThreadPoolExecutor                         # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

PROJ = "/Users/sachoki/Desktop/sachoki-desktop"
LIVE = f"{PROJ}/data"
TARGET = f"{PROJ}/data.INTERNAL_BACKUP_PRE_EXTERNAL_MIGRATION"
PENDING = f"{PROJ}/data.INTERNAL_BACKUP_DELETE_PENDING_20260825"
PENDING_PREFIX = "data.INTERNAL_BACKUP_DELETE_PENDING_"
VOLUME = "/Volumes/QUANT_RESEARCH"
CANON = f"{VOLUME}/source_data/studio"
ARCHIVE = f"{VOLUME}/archive/studio_pre_external_migration_2026-08-24"
MASSIVE_MAN = f"{VOLUME}/source_data/massive/canonical_1m/_manifest"
MANIFEST = f"{VOLUME}/artifacts/manifests/SOURCE_POST_COPY.json"
STORES = ["studio_analytics.duckdb", "studio_15m.duckdb", "studio_15m_base.duckdb",
          "studio_1h.duckdb", "studio_4h.duckdb", "studio_1w.duckdb"]
REP = "studio_analytics.duckdb"

R: dict = {}
HOME_DEV = os.stat(os.path.expanduser("~")).st_dev


class Hold(RuntimeError):
    pass


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb", buffering=0) as f:
        while True:
            b = f.read(8 << 20)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def full_verify(root, exp):
    present = {}
    for dp, _, fn in os.walk(root):
        for f in fn:
            present[os.path.relpath(os.path.join(dp, f), root)] = os.path.join(dp, f)
    missing = sorted(set(exp) - set(present))
    extra = sorted(set(present) - set(exp))
    bad = []

    def chk(rel):
        r, p = exp[rel], present[rel]
        if os.path.getsize(p) != r["size"]:
            return rel
        return rel if sha(p) != r["sha256"] else None

    with ThreadPoolExecutor(max_workers=6) as ex:
        bad = [x for x in ex.map(chk, [k for k in exp if k in present]) if x]
    return dict(files_expected=len(exp), files_present=len(present),
                bytes=sum(os.path.getsize(p) for p in present.values()),
                missing=len(missing), extra=len(extra), mismatches=len(bad),
                detail=(missing[:5] + extra[:5] + bad[:5]),
                clean=not (missing or extra or bad))


def free_bytes(path):
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize


def backend_health():
    try:
        import requests
        r = requests.get("http://127.0.0.1:8080/api/health", timeout=8)
        return r.json() if r.status_code == 200 else f"http {r.status_code}"
    except Exception as e:
        return f"unreachable: {type(e).__name__}"


def read_representative():
    try:
        import duckdb
        p = os.path.join(LIVE, REP)
        c = duckdb.connect(p, read_only=True)
        n = c.execute("select count(*) from bars").fetchone()[0]
        c.close()
        return dict(readable=True, rows=n, st_dev=os.stat(p).st_dev)
    except Exception as e:
        return dict(readable=False, error=f"{type(e).__name__}: {str(e)[:90]}")


def storage_state():
    return dict(
        live_symlink_present=os.path.islink(LIVE),
        live_resolves_to=os.path.realpath(LIVE) if os.path.lexists(LIVE) else None,
        live_resolves_correctly=(os.path.lexists(LIVE)
                                 and os.path.realpath(LIVE) == os.path.realpath(CANON)),
        volume_is_mount=os.path.ismount(VOLUME),
        phantom_internal_volume=(os.path.isdir(VOLUME)
                                 and (not os.path.ismount(VOLUME)
                                      or os.stat(VOLUME).st_dev == HOME_DEV)),
        all_stores_present=all(os.path.exists(os.path.join(LIVE, s)) for s in STORES),
        massive_manifests=len(os.listdir(MASSIVE_MAN)) if os.path.isdir(MASSIVE_MAN) else 0,
        rollback_archive_present=os.path.isdir(ARCHIVE),
        backend=backend_health(),
        representative=read_representative())


# ── PHASE 1 ────────────────────────────────────────────────────────────────────
def phase1():
    from studio.mount_guard import check_external_volume
    ev = check_external_volume(purpose="internal backup cleanup")
    vol_dev = os.stat(VOLUME).st_dev if os.path.isdir(VOLUME) else None
    tgt_real = os.path.realpath(TARGET) if os.path.lexists(TARGET) else None
    p1 = dict(
        volume_is_mount=os.path.ismount(VOLUME),
        volume_device_differs_from_home=vol_dev is not None and vol_dev != HOME_DEV,
        phantom=bool(os.path.isdir(VOLUME) and (not os.path.ismount(VOLUME)
                                                or vol_dev == HOME_DEV)),
        mount_guard_ok=ev["ok"], mount_guard_checks=ev["checks"],
        live_is_symlink=os.path.islink(LIVE),
        live_resolves_correctly=(os.path.lexists(LIVE)
                                 and os.path.realpath(LIVE) == os.path.realpath(CANON)),
        live_target_on_external=(os.path.exists(LIVE)
                                 and os.stat(LIVE).st_dev == vol_dev),
        target_exists=os.path.isdir(TARGET),
        target_is_real_dir=os.path.isdir(TARGET) and not os.path.islink(TARGET),
        target_not_symlink=not os.path.islink(TARGET),
        target_not_mount=not os.path.ismount(TARGET),
        target_on_internal=(os.path.exists(TARGET)
                            and os.stat(TARGET).st_dev == HOME_DEV),
        target_device_differs_from_external=(os.path.exists(TARGET) and vol_dev is not None
                                             and os.stat(TARGET).st_dev != vol_dev),
        target_realpath=tgt_real,
        target_not_live=tgt_real != os.path.realpath(LIVE),
        target_not_canonical=tgt_real != os.path.realpath(CANON),
        target_not_archive=tgt_real != os.path.realpath(ARCHIVE),
        target_not_ancestor_of_external=not (
            os.path.realpath(CANON).startswith((tgt_real or "\0") + os.sep)
            or os.path.realpath(ARCHIVE).startswith((tgt_real or "\0") + os.sep)),
    )
    p1["pass"] = all(v for k, v in p1.items()
                     if isinstance(v, bool) and k not in ("phantom",)) and not p1["phantom"]
    R["phase1_pre_deletion_gate"] = p1
    if not p1["pass"]:
        raise Hold("phase 1 identity/mount gate failed")


# ── PHASE 2 ────────────────────────────────────────────────────────────────────
def phase2():
    man = json.load(open(MANIFEST))
    exp = {r["path"]: r for r in man["files"]}
    internal = full_verify(TARGET, exp)
    external = full_verify(ARCHIVE, exp)
    ok = (internal["clean"] and external["clean"]
          and internal["files_present"] == 162 and external["files_present"] == 162
          and internal["bytes"] == man["total_bytes"]
          and external["bytes"] == man["total_bytes"])
    R["phase2_fresh_full_verification"] = dict(
        method="every file hashed on BOTH sides — no spot check",
        manifest=dict(tree_digest=man["tree_digest"], files=man["n_files"],
                      bytes=man["total_bytes"]),
        internal_target=internal, external_rollback_archive=external,
        establishes=["A: the internal object about to be destroyed is still the exact "
                     "verified pre-migration backup",
                     "B: the external rollback archive that remains is still "
                     "byte-identical to it"],
        **{"pass": ok})
    if not ok:
        raise Hold("phase 2 fresh full verification failed")


# ── PHASE 3 ────────────────────────────────────────────────────────────────────
def phase3():
    tgt_files = sum(len(f) for _, _, f in os.walk(TARGET))
    tgt_bytes = sum(os.path.getsize(os.path.join(d, f))
                    for d, _, fs in os.walk(TARGET) for f in fs)
    arc_files = sum(len(f) for _, _, f in os.walk(ARCHIVE))
    arc_bytes = sum(os.path.getsize(os.path.join(d, f))
                    for d, _, fs in os.walk(ARCHIVE) for f in fs)
    fb = free_bytes(PROJ)
    R["phase3_pre_deletion_state"] = dict(
        internal_free_bytes_before=fb, internal_free_gib_before=round(fb / 2**30, 2),
        external_free_bytes=free_bytes(VOLUME),
        external_free_gib=round(free_bytes(VOLUME) / 2**30, 2),
        internal_target_file_count=tgt_files, internal_target_bytes=tgt_bytes,
        external_archive_file_count=arc_files, external_archive_bytes=arc_bytes,
        data_symlink_target=os.path.realpath(LIVE),
        external_device_id=os.stat(VOLUME).st_dev, internal_device_id=HOME_DEV,
        storage=storage_state(),
        massive_manifest_expected=1254)


# ── PHASE 4 ────────────────────────────────────────────────────────────────────
def phase4():
    if os.path.lexists(PENDING):
        raise Hold(f"staging path already exists: {PENDING}")
    os.rename(TARGET, PENDING)            # atomic, same filesystem
    R["phase4_atomic_rename"] = dict(
        from_path=TARGET, to_path=PENDING,
        same_filesystem=os.stat(PENDING).st_dev == HOME_DEV,
        destination_pre_existing=False, deleted_yet=False,
        rationale="a verified external copy proves the CONTENT is safe; it says nothing "
                  "about whether code still resolves the old internal PATH. This rename "
                  "makes that failure observable while all 84 GB are still recoverable.")


# ── PHASE 5 ────────────────────────────────────────────────────────────────────
def phase5():
    from studio.mount_guard import check_external_volume, invalidate_cache
    invalidate_cache()
    pend_files = sum(len(f) for _, _, f in os.walk(PENDING))
    pend_bytes = sum(os.path.getsize(os.path.join(d, f))
                     for d, _, fs in os.walk(PENDING) for f in fs)
    st = storage_state()
    ev = check_external_volume(purpose="post-rename check")
    checks = dict(
        original_path_absent=not os.path.lexists(TARGET),
        pending_is_real_dir=os.path.isdir(PENDING) and not os.path.islink(PENDING),
        pending_on_internal=os.stat(PENDING).st_dev == HOME_DEV,
        pending_162_files=pend_files == 162,
        pending_bytes_match=pend_bytes == 89778107215,
        live_symlink_intact=st["live_symlink_present"] and st["live_resolves_correctly"],
        mount_guard_pass=ev["ok"],
        backend_healthy=isinstance(st["backend"], dict)
        and st["backend"].get("status") == "ok",
        representative_readable=st["representative"].get("readable") is True,
        representative_on_external=st["representative"].get("st_dev") != HOME_DEV,
        all_stores_present=st["all_stores_present"],
        massive_manifests_1254=st["massive_manifests"] == 1254,
        rollback_archive_present=st["rollback_archive_present"],
        no_phantom_internal_volume=not st["phantom_internal_volume"],
        no_app_path_resolves_into_pending=os.path.realpath(LIVE) != os.path.realpath(PENDING)
        and not os.path.realpath(LIVE).startswith(os.path.realpath(PENDING) + os.sep),
    )
    R["phase5_post_rename_operational"] = dict(
        pending_file_count=pend_files, pending_bytes=pend_bytes,
        storage=st, checks=checks,
        row_count_note="readability and correct external source are the claim — a row "
                       "count need not match an older value, since a legitimate scheduled "
                       "writer may have run since",
        **{"pass": all(checks.values())})
    if not all(checks.values()):
        # Reversible: put it back before anything becomes unrecoverable.
        try:
            if not os.path.lexists(TARGET):
                os.rename(PENDING, TARGET)
                R["phase5_post_rename_operational"]["rollback"] = \
                    f"renamed back to {TARGET} — all 84 GB retained"
        except Exception as e:
            R["phase5_post_rename_operational"]["rollback"] = \
                f"RENAME-BACK FAILED: {type(e).__name__}: {e}"
        raise Hold("phase 5 post-rename operational check failed")


# ── PHASE 6 ────────────────────────────────────────────────────────────────────
def phase6():
    rp = os.path.realpath(PENDING)
    a = dict(
        is_real_dir=os.path.isdir(PENDING) and not os.path.islink(PENDING),
        not_symlink=not os.path.islink(PENDING),
        not_mount=not os.path.ismount(PENDING),
        on_internal_fs=os.stat(PENDING).st_dev == HOME_DEV,
        basename_prefix_ok=os.path.basename(rp).startswith(PENDING_PREFIX),
        inside_project=rp.startswith(PROJ + os.sep),
        not_project_root=rp != PROJ,
        not_live_data=rp != os.path.realpath(LIVE),
        not_under_volumes=not rp.startswith("/Volumes"),
    )
    if not all(a.values()):
        R["phase6_final_deletion"] = dict(assertions=a, deleted=False)
        raise Hold("phase 6 final identity assertion failed")
    shutil.rmtree(rp)                     # exact literal path; no glob, no parent cleanup
    R["phase6_final_deletion"] = dict(
        assertions=a, deleted_path=rp, deleted=True,
        method="shutil.rmtree on the exact resolved literal path",
        no_glob=True, no_wildcard=True, no_parent_cleanup=True)


# ── PHASE 7 / 8 ────────────────────────────────────────────────────────────────
def phase78(before_free):
    from studio.mount_guard import check_external_volume, invalidate_cache
    invalidate_cache()
    st = storage_state()
    ev = check_external_volume(purpose="post-deletion verification")
    arc_files = sum(len(f) for _, _, f in os.walk(ARCHIVE))
    arc_bytes = sum(os.path.getsize(os.path.join(d, f))
                    for d, _, fs in os.walk(ARCHIVE) for f in fs)
    checks = dict(
        original_backup_absent=not os.path.lexists(TARGET),
        delete_pending_absent=not os.path.lexists(PENDING),
        live_symlink_present=st["live_symlink_present"],
        live_resolves_correctly=st["live_resolves_correctly"],
        no_phantom_internal_volume=not st["phantom_internal_volume"],
        mount_guard_pass=ev["ok"],
        canonical_studio_present=os.path.isdir(CANON),
        representative_readable=st["representative"].get("readable") is True,
        all_stores_present=st["all_stores_present"],
        backend_healthy=isinstance(st["backend"], dict)
        and st["backend"].get("status") == "ok",
        massive_manifests_1254=st["massive_manifests"] == 1254,
        rollback_archive_present=os.path.isdir(ARCHIVE),
        rollback_archive_162_files=arc_files == 162,
        rollback_archive_bytes=arc_bytes == 89778107215,
    )
    after = free_bytes(PROJ)
    R["phase7_post_deletion"] = dict(storage=st, archive_files=arc_files,
                                     archive_bytes=arc_bytes, checks=checks,
                                     archive_reverification="non-destructive file/byte "
                                     "check; the sealed full-SHA verification from phase 2 "
                                     "stands and was not repeated absent any anomaly")
    R["phase8_reclamation"] = dict(
        internal_free_bytes_before=before_free,
        internal_free_bytes_after=after,
        reclaimed_bytes=after - before_free,
        reclaimed_gib=round((after - before_free) / 2**30, 2),
        expected_order_of_magnitude_gib=83.6,
        note="APFS accounting, snapshots and metadata mean reclaimed free space need not "
             "equal logical directory bytes. A large positive reclamation is an "
             "OPERATIONAL OBSERVATION, not a content-integrity proof.")
    if not all(checks.values()):
        raise Hold("phase 7 post-deletion verification failed")


def main():
    verdict, err = "HOLD", None
    try:
        phase1()
        phase2()
        phase3()
        before = R["phase3_pre_deletion_state"]["internal_free_bytes_before"]
        phase4()
        phase5()
        phase6()
        phase78(before)
        verdict = "BACKUP_CLEANUP_COMPLETE"
    except Hold as e:
        err = str(e)
    except Exception as e:
        err = f"{type(e).__name__}: {e}"

    p = dict(
        report_id="STUDIO_INTERNAL_BACKUP_CLEANUP_V1",
        final_verdict=verdict, hold_reason=err,
        authorization_scope="ONE destructive cleanup of exactly the internal "
                            "pre-migration backup directory; nothing else",
        exact_original_target=TARGET, exact_delete_pending_path=PENDING,
        rollback_archive_classification="ROLLBACK_ARCHIVE_NOT_DISASTER_RECOVERY",
        rollback_archive_note="canonical data and the rollback archive share one physical "
                              "SSD. This covers logical failure — corruption, a bad "
                              "update, a code defect, accidental deletion — and does NOT "
                              "cover physical loss of that device.",
        external_archive_deletion_authorised=False,
        prior_evidence=dict(artifact="STUDIO_PRE_MIGRATION_ROLLBACK_ARCHIVE_V1",
                            digest="441e5a4cf9383745",
                            note="that artifact recorded deletion_authorised=false; this "
                                 "is the separate deletion authorisation"),
        untouched=["backend and frontend code", "canonical DuckDB files",
                   "Massive raw payloads and manifests", "sealed research artifacts",
                   "mount guard", "launchd configuration", "current-universe overlay",
                   "MASSIVE_TICKER_LINEAGE_V6", "the external rollback archive"],
        jobs_run="none — no dbupdate, research, backfill, derived-data, densification, "
                 "zero-fill, RTH or EMA/RVOL/T-Z job was executed",
        **R,
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "STUDIO_INTERNAL_BACKUP_CLEANUP_V1.json",
                 required=("report_id", "final_verdict", "authorization_scope",
                           "exact_original_target", "rollback_archive_classification"),
                 supersede=os.path.exists("STUDIO_INTERNAL_BACKUP_CLEANUP_V1.json"))
    print(f"STUDIO_INTERNAL_BACKUP_CLEANUP_V1 · {d} · {verdict}")
    if err:
        print(f"  HOLD: {err}")
    for ph in ("phase1_pre_deletion_gate", "phase2_fresh_full_verification",
               "phase5_post_rename_operational"):
        if ph in R:
            print(f"  {ph}: pass={R[ph].get('pass')}")
    if "phase6_final_deletion" in R:
        print(f"  deleted: {R['phase6_final_deletion'].get('deleted')}")
    if "phase8_reclamation" in R:
        print(f"  reclaimed: {R['phase8_reclamation']['reclaimed_gib']} GiB")
    return 0 if verdict == "BACKUP_CLEANUP_COMPLETE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
