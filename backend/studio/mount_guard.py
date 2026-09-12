"""Explicit mount guard for the canonical external research volume.

WHAT THIS EXISTS TO PREVENT. After the studio data migration, the logical path

    /Users/sachoki/Desktop/sachoki-desktop/data

is a symlink onto an external APFS volume. When that volume is mounted everything
works; when it is absent the symlink dangles and DuckDB refuses to open anything, so
the system fails closed. But that fail-closed behaviour is an ACCIDENT of how macOS
unmounts: it removes /Volumes/QUANT_RESEARCH, leaving nothing for the symlink to reach.
If that directory ever existed as an ordinary folder on the internal disk while the real
volume was absent — created by hand, by an installer, by a stray mkdir — then
duckdb.connect() would happily create an empty database there and the failure would be
SILENT. Canonical data would appear to be written and would in fact be going nowhere.

This module replaces that accident with a check. Nothing here is clever: it asks eight
questions, and if any answer is wrong it raises and stops.

WHY A SENTINEL AND NOT JUST A DIRECTORY TEST. Directory existence proves nothing — a
phantom directory has it too. Mount-point status is better but still not identity: any
mounted volume could occupy that path. So the guard also demands a sentinel file holding
a UUID minted once for this physical volume, and — the part that actually matters —
requires that the sentinel be found on a filesystem that is NOT the one $HOME lives on.
A copied sentinel on the internal disk therefore fails, which is exactly the attack the
phantom-directory scenario represents.

    require_external_volume()      -> evidence dict, or raises ExternalVolumeUnavailable

DESIGN CONSTRAINT. This is deliberately small and has no dependencies beyond the stdlib.
It does not touch path handling anywhere else, it does not know about research logic, and
it never repairs anything. In particular it NEVER calls mkdir on the canonical path: the
one thing a guard must not do is manufacture the condition it is checking for.
"""
from __future__ import annotations

import ctypes
import ctypes.util
import json
import os
import time

# ── sealed identity ────────────────────────────────────────────────────────────
# Minted once, on the physical volume, at migration time. Changing this constant
# means "a different volume is now canonical" and is not a routine edit.
EXPECTED_VOLUME_UUID = "6933dcda-ec60-46ee-a490-d6978f4ad8cf"
EXPECTED_VOLUME_NAME = "QUANT_RESEARCH"
EXPECTED_FILESYSTEM = "apfs"

CANONICAL_VOLUME = "/Volumes/QUANT_RESEARCH"
CANONICAL_LOGICAL = "/Users/sachoki/Desktop/sachoki-desktop/data"
CANONICAL_TARGET_SUBPATH = "source_data/studio"
SENTINEL_NAME = ".quant_research_volume"

# Re-checking on every write would stat the volume thousands of times a second during an
# import. Re-checking never would let a mid-run unmount go unnoticed. A short TTL keeps
# the window bounded; a failure is NEVER cached, so a broken state is re-examined on
# every single call rather than being remembered as broken.
_CACHE_TTL_SEC = 5.0
_cache: dict = {}

# ── statfs(2), for filesystem type and device node without spawning a process ──
_MFSTYPENAMELEN, _MAXPATHLEN = 16, 1024


class _statfs(ctypes.Structure):
    _fields_ = [
        ("f_bsize", ctypes.c_uint32), ("f_iosize", ctypes.c_int32),
        ("f_blocks", ctypes.c_uint64), ("f_bfree", ctypes.c_uint64),
        ("f_bavail", ctypes.c_uint64), ("f_files", ctypes.c_uint64),
        ("f_ffree", ctypes.c_uint64), ("f_fsid", ctypes.c_int32 * 2),
        ("f_owner", ctypes.c_uint32), ("f_type", ctypes.c_uint32),
        ("f_flags", ctypes.c_uint32), ("f_fssubtype", ctypes.c_uint32),
        ("f_fstypename", ctypes.c_char * _MFSTYPENAMELEN),
        ("f_mntonname", ctypes.c_char * _MAXPATHLEN),
        ("f_mntfromname", ctypes.c_char * _MAXPATHLEN),
        ("f_flags_ext", ctypes.c_uint32), ("f_reserved", ctypes.c_uint32 * 7)]


_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
_libc.statfs64.argtypes = [ctypes.c_char_p, ctypes.POINTER(_statfs)]
_libc.statfs64.restype = ctypes.c_int


def _fs_info(path: str) -> dict | None:
    """Filesystem type, device node and mount point for `path`, or None if unavailable."""
    b = _statfs()
    if _libc.statfs64(os.fsencode(path), ctypes.byref(b)) != 0:
        return None
    return dict(fstype=b.f_fstypename.decode(errors="replace"),
                device_node=b.f_mntfromname.decode(errors="replace"),
                mount_point=b.f_mntonname.decode(errors="replace"))


class ExternalVolumeUnavailable(RuntimeError):
    """Raised when the canonical external volume is missing, wrong, or impersonated.

    Carries the full evidence dict so a caller — or a test — can see exactly which of
    the eight checks failed, rather than only that something did.
    """

    def __init__(self, evidence: dict):
        self.evidence = evidence
        failed = [k for k, v in evidence["checks"].items() if not v]
        super().__init__(
            f"canonical external volume unavailable — failed checks: {', '.join(failed)}"
            f"\n  purpose : {evidence.get('purpose') or '(unspecified)'}"
            f"\n  volume  : {evidence['volume']}"
            f"\n  logical : {evidence['logical']}"
            f"\n  detail  : {json.dumps(evidence['detail'], indent=2)}"
            f"\n  NOTHING WAS WRITTEN. This guard never creates the path it is checking.")


def check_external_volume(volume: str = CANONICAL_VOLUME,
                          logical: str = CANONICAL_LOGICAL,
                          expected_uuid: str = EXPECTED_VOLUME_UUID,
                          target_subpath: str = CANONICAL_TARGET_SUBPATH,
                          purpose: str = "") -> dict:
    """Evaluate every check and return the evidence. Never raises, never repairs.

    All paths are parameters so the negative conformance fixtures can point the guard at
    a fake volume root instead of needing write access to /Volumes.
    """
    c: dict[str, bool] = {}
    d: dict = {}
    home = os.path.expanduser("~")

    # 1. the volume path exists at all
    c["volume_exists"] = os.path.isdir(volume)
    d["volume"] = volume

    # 2. it is genuinely a mount point, not an ordinary directory
    #    os.path.ismount compares st_dev/st_ino against the parent — pure stat, no shell
    c["is_mount_point"] = c["volume_exists"] and os.path.ismount(volume)

    # 3. its filesystem is not the one $HOME lives on
    try:
        vol_dev = os.stat(volume).st_dev if c["volume_exists"] else None
        home_dev = os.stat(home).st_dev
    except OSError:
        vol_dev = home_dev = None
    d["volume_st_dev"], d["home_st_dev"] = vol_dev, home_dev
    c["distinct_filesystem_from_home"] = vol_dev is not None and vol_dev != home_dev

    # 4. the live filesystem identity matches what we expect of this volume
    fs = _fs_info(volume) if c["volume_exists"] else None
    d["fs_info"] = fs
    c["volume_identity_matches"] = bool(
        fs and fs["fstype"] == EXPECTED_FILESYSTEM
        and os.path.basename(fs["mount_point"]) == EXPECTED_VOLUME_NAME
        and fs["mount_point"] == os.path.realpath(volume))

    # 5. the sentinel is present
    sent = os.path.join(volume, SENTINEL_NAME)
    c["sentinel_exists"] = os.path.isfile(sent)
    d["sentinel_path"] = sent

    # 6. and carries the sealed UUID
    got_uuid = None
    if c["sentinel_exists"]:
        try:
            got_uuid = json.load(open(sent)).get("QUANT_RESEARCH_VOLUME_ID")
        except Exception as e:                        # unreadable or malformed == not valid
            d["sentinel_error"] = f"{type(e).__name__}: {e}"
    d["sentinel_uuid"], d["expected_uuid"] = got_uuid, expected_uuid
    c["sentinel_uuid_matches"] = got_uuid is not None and got_uuid == expected_uuid

    # 7. the logical path resolves, through its symlink, to the expected target
    want = os.path.join(volume, target_subpath)
    real = os.path.realpath(logical) if os.path.lexists(logical) else None
    d["logical"], d["logical_resolves_to"], d["expected_target"] = logical, real, want
    d["logical_is_symlink"] = os.path.islink(logical)
    c["logical_resolves_to_target"] = real is not None and real == os.path.realpath(want)

    # 8. and that resolved target is physically on the filesystem validated above
    try:
        tgt_dev = os.stat(logical).st_dev if (real and os.path.exists(logical)) else None
    except OSError:
        tgt_dev = None
    d["target_st_dev"] = tgt_dev
    c["target_on_validated_filesystem"] = (
        tgt_dev is not None and vol_dev is not None and tgt_dev == vol_dev)

    return dict(ok=all(c.values()), checks=c, detail=d, volume=volume, logical=logical,
                purpose=purpose, checked_at=time.strftime("%Y-%m-%d %H:%M:%S %Z"))


def require_external_volume(purpose: str = "", **kw) -> dict:
    """Raise ExternalVolumeUnavailable unless every check passes. Returns the evidence.

    A PASS is cached briefly; a FAILURE is never cached, so a bad state is re-evaluated
    on every call instead of being remembered.
    """
    key = (kw.get("volume", CANONICAL_VOLUME), kw.get("logical", CANONICAL_LOGICAL))
    hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < _CACHE_TTL_SEC:
        return hit[1]
    ev = check_external_volume(purpose=purpose, **kw)
    if not ev["ok"]:
        _cache.pop(key, None)
        raise ExternalVolumeUnavailable(ev)
    _cache[key] = (time.monotonic(), ev)
    return ev


def is_canonical_path(path: str, logical: str = CANONICAL_LOGICAL) -> bool:
    """True if `path` lies under the canonical (migrated) data root.

    Deliberately a LEXICAL check on the unresolved path. realpath() would be the obvious
    choice and is the wrong one here: when the volume is absent the path does not resolve
    at all, and a guard that quietly declines to fire precisely when the volume is missing
    would be useless. Comparing the logical strings answers "was this meant to be canonical
    data?", which is the question that decides whether the guard applies.
    """
    a = os.path.abspath(path)
    b = os.path.abspath(logical)
    return a == b or a.startswith(b + os.sep)


def require_for_path(path: str, purpose: str = "") -> dict | None:
    """Enforce the guard only for writes aimed at canonical data; ignore anything else.

    This is what keeps the blast radius small. Scratch databases, test fixtures and
    anything outside the migrated tree are untouched by the guard.
    """
    if not is_canonical_path(path):
        return None
    return require_external_volume(purpose=purpose or f"write to {path}")


def invalidate_cache() -> None:
    """Drop the cached PASS. Used by the conformance tests around mount changes."""
    _cache.clear()


if __name__ == "__main__":
    ev = check_external_volume(purpose="cli self-check")
    for k, v in ev["checks"].items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")
    print(f"\n  MOUNT GUARD {'PASS' if ev['ok'] else 'FAIL'}")
    if not ev["ok"]:
        print(json.dumps(ev["detail"], indent=2))
    raise SystemExit(0 if ev["ok"] else 1)
