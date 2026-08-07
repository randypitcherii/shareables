#!/usr/bin/env python3
"""Run this repo's dbt project inside a serverless Databricks job task.

WHY THIS EXISTS (instead of the managed `dbt_task` type):
  * dbt's version is pinned by uv (pyproject.toml) and upgraded with a normal
    PR -- no job-definition edits, no drift between dev/CI/prod dbt versions.
  * All dbt logs land in the Databricks Jobs UI, the same place every other
    scheduled thing lives.
  * Auth is the job's `run_as` service principal, resolved AT RUNTIME from the
    task's ambient credentials (WorkspaceClient default auth). No OAuth client
    secret is ever provisioned, stored, or rotated -- assigning run_as IS the
    auth setup.
  * The committed profiles.yml (env-var placeholders only) stays the single
    source of truth for every environment.

MODES
  ci-build               download the repo tarball at --git-ref, build + test
                         into the disposable --schema with --fail-fast, defer
                         to prod state when the manifest is available (Slim CI),
                         and ALWAYS drop the schema afterward (try/finally).
  ci-sweep               drop leaked CI schemas older than 3 days.
  prod-source-freshness  fail the run when system.billing is stale.
  prod-build             build the daily-tagged models, capture state to the
                         artifacts volume.
  prod-docs              regenerate dbt docs into the artifacts volume.

CI modes fetch the project at an arbitrary git ref via GitHub's tarball
endpoint (public repo, no git binary needed in the serverless sandbox).
Prod modes copy the bundle-synced workspace files this script deployed with,
so prod always runs exactly what the bundle deployed.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

REPO_PROJECT_SUBDIR = "databricks/demos/dbt"


def log(message: str) -> None:
    print(f"[dbt-runner] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        required=True,
        choices=["ci-build", "ci-sweep", "prod-source-freshness", "prod-build", "prod-docs"],
    )
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--git-ref", default="main", help="repo ref to build (CI modes only)")
    parser.add_argument("--github-repo", default="randypitcherii/shareables")
    parser.add_argument("--artifacts-volume", default="", help="/Volumes/... root for state + docs")
    parser.add_argument(
        "--project-dir",
        default="",
        help=(
            "workspace path of the bundle-deployed project (prod modes; the bundle "
            "passes ${workspace.file_path} -- __file__ is undefined in serverless "
            "spark_python_task execution, so the script cannot locate itself)"
        ),
    )
    parser.add_argument("--usage-history-days", default="", help="override DBT_USAGE_HISTORY_DAYS")
    return parser.parse_args()


def build_dbt_env(args: argparse.Namespace) -> dict[str, str]:
    """Resolve auth from the task's ambient credentials (= the job's run_as
    identity) and assemble the env the committed profiles.yml expects."""
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    auth_header = client.config.authenticate()["Authorization"]
    if not auth_header.startswith("Bearer "):
        sys.exit(f"unexpected Authorization header shape: {auth_header.split(' ')[0]}")

    env = dict(os.environ)
    env.update(
        {
            "DBT_HOST": client.config.host.removeprefix("https://"),
            "DBT_HTTP_PATH": f"/sql/1.0/warehouses/{args.warehouse_id}",
            # DBT_ENV_SECRET_ prefix: dbt masks it in logs and forbids it outside profiles.yml
            "DBT_ENV_SECRET_DATABRICKS_TOKEN": auth_header.removeprefix("Bearer "),
            "DBT_DEFAULT_CATALOG": args.catalog,
            "DBT_DEFAULT_SCHEMA": args.schema,
            "DBT_DEPLOYMENT_ENVIRONMENT": (
                "ci_testing" if args.mode.startswith("ci-") else "production"
            ),
            "UV_LINK_MODE": "copy",
        }
    )
    if args.usage_history_days:
        env["DBT_USAGE_HISTORY_DAYS"] = args.usage_history_days
    return env


def fetch_project_from_github(repo: str, ref: str, workdir: Path) -> Path:
    """Download + extract the repo tarball at `ref` (no git needed)."""
    url = f"https://codeload.github.com/{repo}/tar.gz/{ref}"
    tarball = workdir / "src.tar.gz"
    log(f"downloading {url}")
    urllib.request.urlretrieve(url, tarball)
    extract_dir = workdir / "src"
    with tarfile.open(tarball) as archive:
        archive.extractall(extract_dir)
    top_level = next(extract_dir.iterdir())
    project = top_level / REPO_PROJECT_SUBDIR
    if not (project / "dbt_project.yml").exists():
        sys.exit(f"no dbt project at {project} in ref {ref}")
    return project


def copy_project_from_workspace(project_dir: str, workdir: Path) -> Path:
    """Copy the bundle-synced project to local disk -- dbt writes target/ etc.,
    which we keep off the read-through workspace FUSE."""
    if not project_dir:
        sys.exit("--project-dir is required for prod modes")
    source = Path(project_dir)
    project = workdir / "project"
    shutil.copytree(
        source,
        project,
        ignore=shutil.ignore_patterns(".venv", "target", "dbt_packages", "__pycache__"),
    )
    return project


def run(cmd: list[str], cwd: Path, env: dict[str, str]) -> None:
    log(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def dbt(words: list[str], cwd: Path, env: dict[str, str]) -> None:
    run(["uv", "run", "dbt", *words, "--profiles-dir", "."], cwd, env)


def try_download_prod_manifest(artifacts_volume: str, project: Path) -> bool:
    """Fetch the production manifest for Slim CI deferral; False = full build."""
    if not artifacts_volume:
        return False
    manifest_path = f"{artifacts_volume}/state/latest/manifest.json"
    try:
        from databricks.sdk import WorkspaceClient

        response = WorkspaceClient().files.download(manifest_path)
        state_dir = project / "prod_state"
        state_dir.mkdir(exist_ok=True)
        (state_dir / "manifest.json").write_bytes(response.contents.read())
        log(f"downloaded {manifest_path} -> Slim CI enabled")
        return True
    except Exception as error:  # missing manifest / no access -> full build
        log(f"no production manifest ({error}) -> full build")
        return False


def main() -> None:
    args = parse_args()
    env = build_dbt_env(args)

    with tempfile.TemporaryDirectory(prefix="dbt_runner_") as tmp:
        workdir = Path(tmp)
        # per-run cache: tasks of one job run can land on a shared serverless
        # host under DIFFERENT sandbox users, so a fixed /tmp path 403s for
        # whoever arrives second
        env["UV_CACHE_DIR"] = str(workdir / "uv-cache")
        if args.mode.startswith("ci-"):
            project = fetch_project_from_github(args.github_repo, args.git_ref, workdir)
        else:
            project = copy_project_from_workspace(args.project_dir, workdir)

        run(["uv", "sync", "--python", sys.executable], project, env)
        dbt(["deps"], project, env)

        if args.mode == "ci-build":
            build = ["build", "--fail-fast", "--target", "ci"]
            if try_download_prod_manifest(args.artifacts_volume, project):
                build += ["-s", "state:modified+", "--defer", "--state", "prod_state"]
            try:
                dbt(build, project, env)
            finally:
                # teardown must run whether the build passed or failed; its own
                # failure still fails the task (leaked schemas must be loud)
                dbt(
                    ["run-operation", "drop_schema", "--args", f"{{schema: {args.schema}}}", "--target", "ci"],
                    project,
                    env,
                )
        elif args.mode == "ci-sweep":
            dbt(
                [
                    "run-operation",
                    "drop_stale_ci_schemas",
                    "--args",
                    "{prefix: dbt_rpw_dbt_databricks_reference_pr, older_than_days: 3, dry_run: false}",
                    "--target",
                    "ci",
                ],
                project,
                env,
            )
        elif args.mode == "prod-source-freshness":
            dbt(["source", "freshness", "--target", "prod"], project, env)
        elif args.mode == "prod-build":
            dbt(
                [
                    "build",
                    "-s",
                    "tag:daily",
                    "--target",
                    "prod",
                    "--target-path",
                    f"{args.artifacts_volume}/state/latest",
                ],
                project,
                env,
            )
        elif args.mode == "prod-docs":
            dbt(
                [
                    "docs",
                    "generate",
                    "--target",
                    "prod",
                    "--target-path",
                    f"{args.artifacts_volume}/docs/latest",
                ],
                project,
                env,
            )


if __name__ == "__main__":
    main()
