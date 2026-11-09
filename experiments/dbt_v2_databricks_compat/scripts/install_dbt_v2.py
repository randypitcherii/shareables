"""Install a pinned dbt v2 distribution into its own venv (.dbt-v2/).

dbt v2 ships as a self-contained binary inside a platform wheel. The wheel is
taken from the GitHub release for DBT_V2_VERSION rather than from PyPI so the
experiment can pin a release on the day it ships, before package mirrors
catch up. DBT_V2_DIST picks the distribution: `dbt_oss` (Apache 2.0, the
default here) or `dbt` (the fuller distribution under dbt's own license).

Shared with the Jobs runner (scripts/jobs_dbt_v2_runner.py), so the laptop and
the job install exactly the same artifact.
"""

from __future__ import annotations

import os
import platform
import subprocess
import sys
from pathlib import Path

DEFAULT_VERSION = "2.0.11"
RELEASES = "https://github.com/dbt-labs/dbt-core/releases/download"


def wheel_tag() -> str:
    machine = platform.machine().lower()
    system = platform.system()
    if system == "Darwin":
        return "macosx_11_0_arm64" if machine in ("arm64", "aarch64") else "macosx_10_12_x86_64"
    if system == "Linux":
        return (
            "manylinux_2_28_aarch64" if machine in ("arm64", "aarch64") else "manylinux_2_28_x86_64"
        )
    if system == "Windows":
        return "win_amd64"
    raise SystemExit(f"unsupported platform: {system}/{machine}")


def wheel_url(version: str, dist: str) -> str:
    return f"{RELEASES}/v{version}/{dist}-{version}-cp310-abi3-{wheel_tag()}.whl"


def install(venv: Path, version: str, dist: str) -> Path:
    """Create `venv` (if needed), install the wheel, return the dbt executable."""
    dbt = venv / ("Scripts/dbt.exe" if os.name == "nt" else "bin/dbt")
    if not venv.exists():
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
    pip = venv / ("Scripts/pip" if os.name == "nt" else "bin/pip")
    subprocess.run([str(pip), "install", "--quiet", wheel_url(version, dist)], check=True)
    return dbt


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    version = os.environ.get("DBT_V2_VERSION", DEFAULT_VERSION)
    dist = os.environ.get("DBT_V2_DIST", "dbt_oss")
    dbt = install(root / ".dbt-v2", version, dist)
    out = subprocess.run([str(dbt), "--version"], capture_output=True, text=True, check=True)
    print(out.stdout.strip())


if __name__ == "__main__":
    main()
