"""POST_REBOOT_QUALIFICATION_V1 — run this after a reboot with the external drive attached.

    cd /Users/sachoki/Desktop/sachoki-desktop/backend && .venv/bin/python post_reboot_qualification.py

WHAT A REBOOT TESTS THAT NOTHING ELSE DOES. Every previous check ran on a machine where the
volume was already mounted and the services were already up, in an order this session
established. A cold boot re-establishes all of it from scratch, in an order nobody controls,
and that is the point: the external volume mounts asynchronously while launchd is starting
services in parallel. If the backend wins that race, it comes up with the canonical data
path dangling.

That race is not a defect — it is the exact condition the mount guard exists for, and
failing closed is the correct behaviour. What matters is WHICH failure happens:

    guard refuses, no write occurs, service recovers once mounted     CORRECT
    a phantom /Volumes/QUANT_RESEARCH appears as an ordinary folder   FATAL

So the phantom check below is the one that really matters, and it is deliberately
device-level rather than existence-level: a directory being present at that path proves
nothing, since a correctly mounted volume is also present there. The question is whether it
is a MOUNT POINT on a device that is not the home device.

THE BACKUP IS COMPARED IN FULL AGAIN. 162 files, every byte, against the sealed migration
manifest. A reboot is exactly when a filesystem is most likely to have replayed a journal or
resolved something inconsistently, so this is not the moment to sample.

Read-only: this repairs nothing and starts nothing. If a service is down it is reported as
down.
"""
from __future__ import annotations
import hashlib, json, os, subprocess, sys, time                          # noqa: E402
from concurrent.futures import ThreadPoolExecutor                        # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

PROJ = "/Users/sachoki/Desktop/sachoki-desktop"
LOGICAL = f"{PROJ}/data"
BACKUP = f"{PROJ}/data.INTERNAL_BACKUP_PRE_EXTERNAL_MIGRATION"
VOLUME = "/Volumes/QUANT_RESEARCH"
TARGET = f"{VOLUME}/source_data/studio"
MANIFEST = f"{VOLUME}/artifacts/manifests/SOURCE_POST_COPY.json"
REPRESENTATIVE = "studio_analytics.duckdb"
MASSIVE = f"{VOLUME}/source_data/massive/canonical_1m/_manifest"


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb", buffering=0) as f:
        while True:
            b = f.read(8 << 20)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def uptime_seconds():
    out = subprocess.run(["sysctl", "-n", "kern.boottime"], capture_output=True,
                         text=True).stdout
    try:
        sec = int(out.split("sec = ")[1].split(",")[0])
        return int(time.time()) - sec
    except Exception:
        return None


def main():
    home_dev = os.stat(os.path.expanduser("~")).st_dev
    up = uptime_seconds()

    # 1 volume mounted, and NOT a phantom directory
    vol_exists = os.path.isdir(VOLUME)
    is_mount = vol_exists and os.path.ismount(VOLUME)
    vol_dev = os.stat(VOLUME).st_dev if vol_exists else None
    phantom = bool(vol_exists and (not is_mount or vol_dev == home_dev))
    volume = dict(path=VOLUME, exists=vol_exists, is_mount_point=is_mount,
                  st_dev=vol_dev, home_st_dev=home_dev,
                  phantom_internal_folder=phantom,
                  note="existence proves nothing — a correctly mounted volume is also "
                       "present here. The test is mount-point status on a non-home device.")

    # 2 symlink resolves to the external target
    link = dict(logical=LOGICAL, is_symlink=os.path.islink(LOGICAL),
                resolves_to=os.path.realpath(LOGICAL) if os.path.lexists(LOGICAL) else None,
                expected=TARGET,
                resolves_correctly=(os.path.lexists(LOGICAL)
                                    and os.path.realpath(LOGICAL)
                                    == os.path.realpath(TARGET)))

    # 3 mount guard
    from studio.mount_guard import check_external_volume
    ev = check_external_volume(purpose="post-reboot qualification")
    guard = dict(ok=ev["ok"], checks=ev["checks"])

    # 4 services
    lc = subprocess.run(["launchctl", "list"], capture_output=True, text=True).stdout
    svc = {l.split()[2]: dict(pid=l.split()[0], last_exit=l.split()[1])
           for l in lc.splitlines() if "sachoki" in l}
    health = None
    try:
        import requests
        r = requests.get("http://127.0.0.1:8080/api/health", timeout=8)
        health = r.json() if r.status_code == 200 else f"http {r.status_code}"
    except Exception as e:
        health = f"unreachable: {type(e).__name__}"
    services = dict(loaded=svc, backend_health=health,
                    backend_running=svc.get("com.sachoki.backend", {}).get("pid", "-")
                    not in ("-", None))

    # 5 representative read from the external volume
    rep = dict(store=REPRESENTATIVE)
    try:
        import duckdb
        p = os.path.join(LOGICAL, REPRESENTATIVE)
        c = duckdb.connect(p, read_only=True)
        rep.update(readable=True, rows=c.execute("select count(*) from bars").fetchone()[0],
                   st_dev=os.stat(p).st_dev,
                   on_external=os.stat(p).st_dev != home_dev)
        c.close()
    except Exception as e:
        rep.update(readable=False, error=f"{type(e).__name__}: {str(e)[:90]}")

    # 6 internal backup — FULL comparison
    backup = dict(comparison="FULL — every file hashed")
    try:
        man = json.load(open(MANIFEST))
        exp = {r["path"]: r for r in man["files"]}
        present = {}
        for dp, _, fn in os.walk(BACKUP):
            for f in fn:
                present[os.path.relpath(os.path.join(dp, f), BACKUP)] = \
                    os.path.join(dp, f)
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
        backup.update(files_expected=len(exp), files_present=len(present),
                      missing=len(missing), extra=len(extra), mismatches=len(bad),
                      mutation_since_migration=len(missing) + len(extra) + len(bad),
                      detail=(missing[:5] + extra[:5] + bad[:5]))
    except Exception as e:
        backup.update(error=f"{type(e).__name__}: {str(e)[:90]}")

    # 7 massive archive still intact at the file-count level
    massive = dict(manifest_dir=MASSIVE,
                   manifests=len(os.listdir(MASSIVE)) if os.path.isdir(MASSIVE) else 0,
                   expected=1254)

    checks = dict(
        volume_mounted=is_mount,
        volume_on_external_device=vol_dev is not None and vol_dev != home_dev,
        no_phantom_internal_folder=not phantom,
        symlink_resolves_correctly=link["resolves_correctly"],
        mount_guard_pass=guard["ok"],
        services_all_loaded=len(svc) == 4,
        backend_healthy=isinstance(health, dict) and health.get("status") == "ok",
        representative_store_readable=rep.get("readable") is True,
        representative_store_on_external=rep.get("on_external") is True,
        backup_mutation_zero=backup.get("mutation_since_migration") == 0,
        massive_archive_intact=massive["manifests"] == massive["expected"])

    p = dict(
        report_id="POST_REBOOT_QUALIFICATION_V1",
        status="PASS" if all(checks.values()) else "HOLD",
        uptime_seconds=up,
        uptime_note=("this looks like a fresh boot" if up is not None and up < 7200
                     else "WARNING: uptime suggests the machine may NOT have been "
                          "rebooted since the last qualification — the boot-order race "
                          "this test exists for would then not have been exercised"),
        boot_order_race=dict(
            why_it_matters="the external volume mounts asynchronously while launchd starts "
                           "services in parallel. If the backend wins that race it comes "
                           "up with the canonical data path dangling.",
            correct_outcome="the guard refuses, no write occurs, and the service recovers "
                            "once the volume is mounted",
            fatal_outcome="a phantom /Volumes/QUANT_RESEARCH exists as an ordinary folder "
                          "on the internal disk",
            observed_phantom=phantom),
        volume=volume, symlink=link, mount_guard=guard, services=services,
        representative_read=rep, internal_backup=backup, massive_archive=massive,
        acceptance=checks,
        read_only="this qualification repairs nothing and starts nothing; a service that is "
                  "down is reported as down",
        backup_retention="the 84 GB internal backup remains RETAINED. Deletion is a "
                         "SEPARATE cleanup gate and is not authorised by this artifact.",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "POST_REBOOT_QUALIFICATION_V1.json",
                 required=("report_id", "status", "volume", "symlink", "mount_guard",
                           "internal_backup", "acceptance"),
                 supersede=os.path.exists("POST_REBOOT_QUALIFICATION_V1.json"))
    print(f"POST_REBOOT_QUALIFICATION_V1 · {d} · {p['status']}")
    print(f"  uptime {up}s — {p['uptime_note']}")
    print(f"  volume mounted={is_mount} dev={vol_dev} phantom={phantom}")
    print(f"  symlink -> {link['resolves_to']}")
    print(f"  guard {guard['ok']} · backend {health}")
    print(f"  {REPRESENTATIVE}: readable={rep.get('readable')} rows={rep.get('rows')}")
    print(f"  backup: {backup.get('files_present')}/{backup.get('files_expected')} · "
          f"MUTATION {backup.get('mutation_since_migration')}")
    print(f"  massive manifests {massive['manifests']}/{massive['expected']}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
