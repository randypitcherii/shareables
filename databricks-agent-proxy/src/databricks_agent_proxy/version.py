"""Git-hash based versioning per project standards."""

import subprocess
from pathlib import Path
from typing import Optional

_git_info_cache: Optional[tuple] = None

SEMVER = "0.1.0"


def get_git_info() -> tuple[Optional[str], Optional[str]]:
    """Return (short_hash, commit_date), cached after first call."""
    global _git_info_cache
    if _git_info_cache is not None:
        return _git_info_cache

    for cwd in [Path(__file__).parent.parent.parent, Path.cwd()]:
        try:
            hash_r = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                timeout=1,
                cwd=str(cwd),
            )
            date_r = subprocess.run(
                ["git", "log", "-1", "--format=%as"],
                capture_output=True,
                text=True,
                timeout=1,
                cwd=str(cwd),
            )
            if hash_r.returncode == 0:
                h = hash_r.stdout.strip()
                d = date_r.stdout.strip() if date_r.returncode == 0 else None
                _git_info_cache = (h, d)
                return _git_info_cache
        except Exception:
            continue

    _git_info_cache = (None, None)
    return _git_info_cache


def version_dict() -> dict:
    """Return version info as a dict for API responses."""
    git_hash, git_date = get_git_info()
    return {
        "version": SEMVER,
        "git_hash": git_hash or "unknown",
        "git_commit_date": git_date or "unknown",
    }
