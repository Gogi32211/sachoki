"""STUDIO_PRE_MIGRATION_ROLLBACK_ARCHIVE_V1 — verify the external copy before anything is
deleted internally.

THIS IS A ROLLBACK ARCHIVE, NOT A DISASTER-RECOVERY BACKUP, and the distinction is not
pedantry — it determines what this artifact may later be used to justify.

    protects against    DuckDB corruption · a bad update · a code bug · accidental
                        deletion · needing the pre-migration state back
    does NOT protect    physical failure of the QUANT_RESEARCH SSD

Canonical data and this archive live on the SAME physical device. If that drive dies, both
go together. Nothing in this report should be read as evidence that the data is redundantly
stored.

FULL COMPARISON, NOT A SAMPLE. All 162 files are hashed against the sealed migration
manifest. A spot check was adequate for progress reporting; this artifact is what would
license deleting 84 GB, so it reads every byte.

BOTH SIDES ARE CHECKED. The external copy is verified against the manifest, and so is the
internal source — because "the copy matches the manifest" and "the internal original is
still intact" are different claims, and only the second one makes deletion safe to consider.

Read-only. Deletes nothing, and does not authorise deletion.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                      # noqa: E402
from concurrent.futures import ThreadPoolExecutor                        # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

INTERNAL = "/Users/sachoki/Desktop/sachoki-desktop/data.INTERNAL_BACKUP_PRE_EXTERNAL_MIGRATION"
ARCHIVE = "/Volumes/QUANT_RESEARCH/archive/studio_pre_external_migration_2026-08-24"
MANIFEST = "/Volumes/QUANT_RESEARCH/artifacts/manifests/SOURCE_POST_COPY.json"


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb", buffering=0) as f:
        while True:
            b = f.read(8 << 20)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def compare(root, exp):
    present = {}
    for dp, _, fn in os.walk(root):
        for f in fn:
            present[os.path.relpath(os.path.join(dp, f), root)] = os.path.join(dp, f)
    missing = sorted(set(exp) - set(present))
    extra = sorted(set(present) - set(exp))
    size_bad, sha_bad = [], []

    def chk(rel):
        r, p = exp[rel], present[rel]
        if os.path.getsize(p) != r["size"]:
            return ("size", rel)
        return ("sha", rel) if sha(p) != r["sha256"] else None

    with ThreadPoolExecutor(max_workers=6) as ex:
        for res in ex.map(chk, [k for k in exp if k in present]):
            if res and res[0] == "size":
                size_bad.append(res[1])
            elif res:
                sha_bad.append(res[1])
    total = sum(os.path.getsize(p) for p in present.values())
    return dict(files_expected=len(exp), files_present=len(present),
                bytes=total, missing=len(missing), extra=len(extra),
                size_mismatches=len(size_bad), sha256_mismatches=len(sha_bad),
                detail=(missing[:5] + extra[:5] + size_bad[:5] + sha_bad[:5]),
                clean=not (missing or extra or size_bad or sha_bad))


def main():
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose="rollback archive verification")
    t0 = time.time()
    man = json.load(open(MANIFEST))
    exp = {r["path"]: r for r in man["files"]}

    ext = compare(ARCHIVE, exp)
    internal = compare(INTERNAL, exp)

    checks = dict(
        archive_files_162=ext["files_present"] == 162 and ext["files_expected"] == 162,
        archive_bytes_match=ext["bytes"] == man["total_bytes"],
        archive_no_missing=ext["missing"] == 0,
        archive_no_extra=ext["extra"] == 0,
        archive_no_size_mismatch=ext["size_mismatches"] == 0,
        archive_no_sha_mismatch=ext["sha256_mismatches"] == 0,
        internal_source_still_intact=internal["clean"],
        archive_on_external_device=(os.stat(ARCHIVE).st_dev
                                    != os.stat(os.path.expanduser("~")).st_dev))

    p = dict(
        report_id="STUDIO_PRE_MIGRATION_ROLLBACK_ARCHIVE_V1",
        status="VERIFIED" if all(checks.values()) else "HOLD",
        classification=dict(
            what_this_is="ROLLBACK ARCHIVE",
            what_this_is_not="DISASTER-RECOVERY BACKUP",
            reason="canonical data and this archive are on the SAME physical device "
                   "(QUANT_RESEARCH). Physical failure of that SSD loses both.",
            protects_against=["DuckDB corruption", "a bad update", "a code bug",
                              "accidental deletion",
                              "needing the pre-migration state back"],
            does_not_protect_against=["physical failure of the QUANT_RESEARCH SSD"],
            binding="this report must never be cited as evidence that the data is "
                    "redundantly stored"),
        source=dict(path=INTERNAL, verification=internal),
        archive=dict(path=ARCHIVE, verification=ext),
        manifest=dict(artifact=os.path.basename(MANIFEST),
                      tree_digest=man["tree_digest"], files=man["n_files"],
                      bytes=man["total_bytes"]),
        comparison="FULL — all 162 files hashed on BOTH sides; a spot check was adequate "
                   "for progress reporting, but this is the artifact that would license "
                   "deleting 84 GB",
        both_sides_checked="the external copy matches the manifest AND the internal source "
                           "is still intact — different claims, and only the second makes "
                           "deletion safe to consider",
        internal_backup_state="RETAINED — nothing has been deleted",
        deletion_authorised=False,
        deletion_note="this artifact verifies the copy. It does NOT authorise deleting the "
                      "internal 84 GB; that remains a separate, explicitly-approved step.",
        elapsed_sec=round(time.time() - t0, 1),
        acceptance=checks,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(p, "STUDIO_PRE_MIGRATION_ROLLBACK_ARCHIVE_V1.json",
                 required=("report_id", "status", "classification", "source", "archive",
                           "acceptance"),
                 supersede=os.path.exists("STUDIO_PRE_MIGRATION_ROLLBACK_ARCHIVE_V1.json"))
    print(f"STUDIO_PRE_MIGRATION_ROLLBACK_ARCHIVE_V1 · {d} · {p['status']}")
    print(f"  archive : {ext['files_present']}/{ext['files_expected']} files · "
          f"{ext['bytes']:,} bytes · missing {ext['missing']} · extra {ext['extra']} · "
          f"size {ext['size_mismatches']} · sha {ext['sha256_mismatches']}")
    print(f"  internal: {internal['files_present']}/{internal['files_expected']} files · "
          f"clean={internal['clean']}")
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    print(f"  elapsed {p['elapsed_sec']/60:.1f} min · deletion_authorised={p['deletion_authorised']}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
