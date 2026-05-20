"""Build marker: identifies which code version produced an artifact.

The build marker is read at module-import time and includes:
- TZ_WLNBB_VERSION constant
- Short git commit SHA (if git is available)
- Build timestamp (when the module was first imported)

This marker is embedded into:
- ZIP metadata (replay_tz_wlnbb_metadata.json + tz_wlnbb_config_snapshot.json)
- CSV filenames returned by the export endpoints
- Every CSV export's first row (build_marker column when present)
- The /api/code-version endpoint response

If an uploaded artifact's build_marker matches the deployed code's marker,
the file definitely came from the current code. If they differ, the file
is stale.
"""
import os
import subprocess
from datetime import datetime, timezone

from .config import TZ_WLNBB_VERSION


def _git_short_sha() -> str:
    try:
        repo_root = os.path.join(os.path.dirname(__file__), "..", "..", "..")
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root, capture_output=True, text=True, timeout=2,
        )
        sha = (result.stdout or "").strip()
        if sha:
            return sha
    except Exception:
        pass
    # Fallback: read .git/HEAD directly
    try:
        head_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", ".git", "HEAD")
        with open(head_path, "r") as f:
            ref = f.read().strip()
        if ref.startswith("ref: "):
            ref_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", ".git", ref[5:])
            with open(ref_path, "r") as f:
                return f.read().strip()[:7]
        return ref[:7]
    except Exception:
        return "unknown"


_BUILD_TIME_UTC = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
_GIT_SHA = _git_short_sha()

BUILD_MARKER = f"{TZ_WLNBB_VERSION}__sha-{_GIT_SHA}__built-{_BUILD_TIME_UTC}"
BUILD_INFO = {
    "tz_wlnbb_version": TZ_WLNBB_VERSION,
    "git_sha": _GIT_SHA,
    "build_time_utc": _BUILD_TIME_UTC,
    "build_marker": BUILD_MARKER,
}


def get_build_marker() -> str:
    """Returns the active build marker string."""
    return BUILD_MARKER


def get_build_info() -> dict:
    """Returns the active build info dict (suitable for JSON serialization)."""
    return dict(BUILD_INFO)


def filename_marker() -> str:
    """Filesystem-safe slug for use in filenames. Returns short form to keep
    paths reasonable: sha-<7chars>_built-<YYYYMMDDTHHMMSSZ>"""
    return f"sha-{_GIT_SHA}_built-{_BUILD_TIME_UTC}"
