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
                         into the disposable --schema with --fail-fast (Slim CI
                         deferral when the prod manifest is available), then --
                         ONLY after a green build -- run the ci_cleanup
                         run-operation with dry_run=False, dropping this build's
                         schema and sweeping stale ones. A FAILED build leaves
                         its schema up for debugging; the next green run's
                         sweep reclaims it.
  cd                     continuous deployment, on merge to production: download
                         the repo tarball at --git-ref, `databricks bundle deploy
                         -t prod` (the DAB components redeploy themselves, as
                         this job's run_as SP), then slim-build only what changed
                         (state:modified+ --fail-fast vs captured prod state;
                         full build when no state exists) and refresh the state.
  prod-source-freshness  fail the run when system.billing is stale.
  prod-build             build the models selected by --build-select (a cadence
                         tag: tag:daily, tag:hourly); capture state to the
                         artifacts volume when --artifacts-volume is given.
  prod-docs              regenerate dbt docs into the artifacts volume.

CI and CD modes fetch the project at an arbitrary git ref via GitHub's tarball
endpoint (public repo, no git binary needed in the serverless sandbox).
The other prod modes copy the bundle-synced workspace files this script
deployed with, so scheduled prod runs execute exactly what the bundle deployed.
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
DATABRICKS_CLI_VERSION = "1.3.0"


def log(message: str) -> None:
    print(f"[dbt-runner] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        required=True,
        choices=["ci-build", "cd", "prod-source-freshness", "prod-build", "prod-docs"],
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
    parser.add_argument(
        "--build-select",
        default="tag:daily",
        help="dbt selector for prod-build (a cadence tag: tag:daily, tag:hourly)",
    )
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


def install_databricks_cli(workdir: Path) -> Path:
    """Download the standalone Databricks CLI (the sandbox has none) for the
    `cd` mode's bundle deploy. Returns the binary path.

    Serverless hosts are a MIX of x86_64 and arm64 -- download for the arch
    this run actually landed on, or execve fails with 'Exec format error'."""
    import platform

    arch = "arm64" if platform.machine().lower() in {"arm64", "aarch64"} else "amd64"
    url = (
        "https://github.com/databricks/cli/releases/download/"
        f"v{DATABRICKS_CLI_VERSION}/databricks_cli_{DATABRICKS_CLI_VERSION}_linux_{arch}.tar.gz"
    )
    cli_dir = workdir / "cli"
    cli_dir.mkdir(exist_ok=True)
    binary = cli_dir / "databricks"
    if binary.exists():  # cd mode uses the CLI more than once per run
        return binary
    tarball = cli_dir / "cli.tar.gz"
    log(f"downloading {url}")
    urllib.request.urlretrieve(url, tarball)
    with tarfile.open(tarball) as archive:
        archive.extractall(cli_dir)
    return cli_dir / "databricks"


def prod_bundle_cli(workdir: Path) -> tuple[Path, dict[str, str]]:
    """The standalone CLI plus a minimal, explicitly token-authed env for prod
    bundle commands run as the job's run_as identity. The sandbox's ambient
    auth vars would otherwise make the CLI complain about conflicting auth
    types."""
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    token = client.config.authenticate()["Authorization"].removeprefix("Bearer ")
    cli = install_databricks_cli(workdir)
    cli_env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(workdir / "cli_home"),
        "DATABRICKS_HOST": client.config.host,
        "DATABRICKS_TOKEN": token,
        "DATABRICKS_AUTH_TYPE": "pat",
    }
    Path(cli_env["HOME"]).mkdir(exist_ok=True)
    return cli, cli_env


def bundle_deploy_prod(project: Path, workdir: Path) -> None:
    """Redeploy this bundle's prod target as the job's run_as identity."""
    from datetime import date

    cli, cli_env = prod_bundle_cli(workdir)
    run(
        [str(cli), "bundle", "deploy", "-t", "prod", f"--var=deployed_at={date.today().isoformat()}"],
        project,
        cli_env,
    )


def bundle_run_docs_app(project: Path, workdir: Path) -> None:
    """Ship docs_app/ code changes. `bundle deploy` only updates the app
    RESOURCE (config, permissions, secrets) and syncs source files -- it never
    creates an app code DEPLOYMENT, so app.py changes would otherwise sit
    undeployed until someone runs one by hand (#80). `bundle run` on the app
    resource deploys the just-synced source and (re)starts the app; the brief
    docs-site restart per merge is acceptable here."""
    cli, cli_env = prod_bundle_cli(workdir)
    run([str(cli), "bundle", "run", "dbt_docs", "-t", "prod"], project, cli_env)


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
        if args.mode.startswith("ci-") or args.mode == "cd":
            project = fetch_project_from_github(args.github_repo, args.git_ref, workdir)
        else:
            project = copy_project_from_workspace(args.project_dir, workdir)

        run(["uv", "sync", "--python", sys.executable], project, env)
        dbt(["deps"], project, env)

        if args.mode == "ci-build":
            build = ["build", "--fail-fast", "--target", "ci"]
            if try_download_prod_manifest(args.artifacts_volume, project):
                build += ["-s", "state:modified+", "--defer", "--state", "prod_state"]
            dbt(build, project, env)
            # cleanup runs ONLY after a green build: a failed build leaves its
            # schema up for debugging, and the sweep inside ci_cleanup (stale
            # prefix-matched schemas) reclaims it on the next green run.
            # dry_run=False because ci_cleanup, like every destructive
            # run-operation here, defaults to a printing-only dry run.
            dbt(
                [
                    "run-operation",
                    "ci_cleanup",
                    "--args",
                    f"{{schema: {args.schema}, dry_run: False}}",
                    "--target",
                    "ci",
                ],
                project,
                env,
            )
        elif args.mode == "cd":
            # deploy first: the merged SHA's job/schema/volume/app definitions
            # go live before any model builds against them
            bundle_deploy_prod(project, workdir)
            build = ["build", "--fail-fast", "--target", "prod"]
            if args.artifacts_volume:
                # refresh captured state so the next CI/CD diffs against what
                # is now live (the manifest describes the whole project at the
                # merged SHA regardless of how few models this run builds)
                build += ["--target-path", f"{args.artifacts_volume}/state/latest"]
            if try_download_prod_manifest(args.artifacts_volume, project):
                build += ["-s", "state:modified+", "--state", "prod_state"]
            dbt(build, project, env)
            if args.artifacts_volume:
                # refresh the hosted docs too: without this, a merge is live in
                # the catalog but invisible on the docs site until the next
                # scheduled prod-docs run (same path prod-docs writes)
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
            # ship docs_app/ code too -- bundle deploy alone never does (#80)
            bundle_run_docs_app(project, workdir)
        elif args.mode == "prod-source-freshness":
            dbt(["source", "freshness", "--target", "prod"], project, env)
        elif args.mode == "prod-build":
            build = ["build", "-s", args.build_select, "--target", "prod"]
            if args.artifacts_volume:
                build += ["--target-path", f"{args.artifacts_volume}/state/latest"]
            dbt(build, project, env)
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
