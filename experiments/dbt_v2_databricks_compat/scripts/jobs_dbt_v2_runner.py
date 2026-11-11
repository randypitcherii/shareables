"""Run the dbt v2 project inside a serverless Lakeflow Jobs Python task.

The serverless environment installs the pinned dbt v2 wheel (see
databricks.yml), which puts the self-contained `dbt` binary next to the task's
Python interpreter. This script only has to:

  1. copy the bundle-synced project to local disk (dbt writes target/ and
     dbt_packages/ next to the project),
  2. resolve a short-lived token for the job's run_as identity from the task's
     ambient credentials, and the warehouse's HTTP path,
  3. run `dbt deps` and `dbt build`, streaming output to the Jobs UI, and fail
     the task when dbt fails.

The committed profiles.yml reads everything from environment variables, so
the same profile serves the laptop and the job.
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def find_dbt() -> str:
    beside_python = Path(sys.executable).parent / "dbt"
    if beside_python.exists():
        return str(beside_python)
    found = shutil.which("dbt")
    if not found:
        raise SystemExit("dbt binary not found: is the dbt v2 wheel in the task environment?")
    return found


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--landing-path", required=True)
    parser.add_argument("--vars", default="{batch: 1}")
    args = parser.parse_args()

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    token = w.config.authenticate()["Authorization"].removeprefix("Bearer ")
    http_path = w.warehouses.get(args.warehouse_id).odbc_params.path

    work = Path(tempfile.mkdtemp()) / "dbt_project"
    shutil.copytree(args.project_dir, work, ignore=shutil.ignore_patterns("target", "dbt_packages"))
    env = {
        **os.environ,
        "DBT_HOST": w.config.host.removeprefix("https://"),
        "DBT_HTTP_PATH": http_path,
        "DBT_CATALOG": args.catalog,
        "DBT_SCHEMA": args.schema,
        "DBT_LANDING_PATH": args.landing_path,
        "DBT_ENV_SECRET_DATABRICKS_TOKEN": token,
        "DO_NOT_TRACK": "1",
    }
    dbt = find_dbt()
    print(f"[dbt-v2-runner] platform: {platform.system()}/{platform.machine()}", flush=True)
    for cmd in (["--version"], ["deps"], ["build", "--full-refresh", "--vars", args.vars]):
        print(f"[dbt-v2-runner] $ dbt {' '.join(cmd)}", flush=True)
        argv = [dbt, *cmd, "--profiles-dir", str(work)]
        rc = subprocess.run(argv, cwd=work, env=env, check=False).returncode
        if rc != 0:
            raise SystemExit(f"dbt {cmd[0]} failed with exit code {rc}")


if __name__ == "__main__":
    main()
