"""Shared helpers: auth, output style, SQL execution, results writing.

Every live claim in this experiment's findings matrix is produced by a script
that goes through this module, so auth handling and evidence capture stay
uniform.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState

EXPERIMENT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_PATH = EXPERIMENT_ROOT / "results" / "matrix_results.json"


def _load_dotenv() -> None:
    """Load the gitignored .env written by scripts/setup/00_*.py.

    Make's `-include .env` is evaluated when the Makefile is parsed, which is
    before `make setup` has written the file — so the setup and matrix targets
    would not see the caller credentials in the same invocation. Loading here
    makes each script self-sufficient.
    """
    env_path = EXPERIMENT_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ[key.strip()] = value.strip()


_load_dotenv()

PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
CATALOG = os.environ.get("EXPERIMENT_CATALOG", "my_catalog")
SCHEMA = os.environ.get("EXPERIMENT_SCHEMA", "udf_privilege_delegation")
WAREHOUSE_ID = os.environ.get("EXPERIMENT_WAREHOUSE_ID", "")

FQ_SCHEMA = f"`{CATALOG}`.`{SCHEMA}`"

# The low-privilege caller identity, populated by scripts/setup/00_*.py.
LOWPRIV_DISPLAY_NAME = os.environ.get(
    "EXPERIMENT_LOWPRIV_DISPLAY_NAME", "udf-delegation-lowpriv"
)


# --------------------------------------------------------------------------
# output
# --------------------------------------------------------------------------


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def step(msg: str) -> None:
    print(f"  ->  {msg}")


def ok(msg: str) -> None:
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    print(f"  !!  {msg}")


def info(msg: str) -> None:
    print(f"      {msg}")


# --------------------------------------------------------------------------
# auth
# --------------------------------------------------------------------------


def _refuse_pat(value: str | None, where: str) -> None:
    """This repo is public and SSO/OAuth-only. Personal access tokens are a
    footgun that ends up in shell history and result artifacts."""
    if value and value.startswith("dapi"):
        raise SystemExit(
            f"Refusing to run: a personal access token was supplied via {where}. "
            "This experiment is OAuth/SSO only — use a Databricks CLI profile."
        )


def admin_client() -> WorkspaceClient:
    """The high-privilege author identity (a workspace admin, via SSO profile)."""
    _refuse_pat(os.environ.get("DATABRICKS_TOKEN"), "DATABRICKS_TOKEN")
    return WorkspaceClient(profile=PROFILE)


def lowpriv_client() -> WorkspaceClient:
    """The low-privilege caller identity — an OAuth M2M service principal that
    holds nothing but USE CATALOG / USE SCHEMA / EXECUTE.

    Credentials come from the gitignored .env written by
    scripts/setup/00_create_lowpriv_principal.py.
    """
    client_id = os.environ.get("EXPERIMENT_LOWPRIV_CLIENT_ID")
    client_secret = os.environ.get("EXPERIMENT_LOWPRIV_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise SystemExit(
            "EXPERIMENT_LOWPRIV_CLIENT_ID / _CLIENT_SECRET are unset. "
            "Run `make setup` first — it creates the low-privilege service "
            "principal and writes .env."
        )
    # auth_type is pinned: DATABRICKS_CONFIG_PROFILE is exported by the Makefile,
    # and without pinning, the SDK's unified auth resolves the CLI profile first
    # and silently hands back the *admin* identity — which would make every row
    # of this matrix a self-test.
    return WorkspaceClient(
        host=admin_client().config.host,
        client_id=client_id,
        client_secret=client_secret,
        auth_type="oauth-m2m",
    )


def assert_distinct_identities(admin: WorkspaceClient, caller: WorkspaceClient) -> tuple[str, str]:
    """Refuse to produce evidence unless the two clients really are two identities.

    The entire experiment is worthless if the "low-privilege caller" is actually
    the admin's session, so this is checked against the live warehouse rather
    than assumed from configuration.
    """
    admin_who = run_sql(admin, "SELECT current_user()").scalar
    caller_who = run_sql(caller, "SELECT current_user()").scalar
    if not caller_who or admin_who == caller_who:
        raise SystemExit(
            "Identity check failed: the low-privilege client resolved to "
            f"{caller_who!r}, the same identity as the admin client. Every matrix "
            "row would be a self-test. Check .env and the oauth-m2m credentials."
        )
    return str(admin_who), str(caller_who)


def warehouse_id(w: WorkspaceClient) -> str:
    """Resolve the SQL warehouse to run statements on."""
    if WAREHOUSE_ID:
        return WAREHOUSE_ID
    for wh in w.warehouses.list():
        if wh.enable_serverless_compute and wh.id:
            return wh.id
    raise SystemExit(
        "No serverless SQL warehouse found. Set EXPERIMENT_WAREHOUSE_ID."
    )


# --------------------------------------------------------------------------
# sql
# --------------------------------------------------------------------------


class SqlOutcome:
    """The outcome of one statement — success rows, or the platform's refusal.

    Failure output is data in this experiment: *which* error the platform
    raises (PERMISSION_DENIED vs UNAUTHORIZED_ACCESS vs a sandbox error)
    is often the finding itself.
    """

    def __init__(
        self,
        succeeded: bool,
        rows: list[list[Any]] | None = None,
        error: str | None = None,
        error_code: str | None = None,
    ) -> None:
        self.succeeded = succeeded
        self.rows = rows or []
        self.error = error
        self.error_code = error_code

    @property
    def scalar(self) -> Any:
        return self.rows[0][0] if self.rows and self.rows[0] else None

    def summary(self) -> dict[str, Any]:
        return {
            "succeeded": self.succeeded,
            "scalar": self.scalar,
            "error_code": self.error_code,
            "error": _truncate(self.error) if self.error else None,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"SqlOutcome(succeeded={self.succeeded}, scalar={self.scalar!r})"


def _truncate(text: str, limit: int = 600) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[:limit] + " ...[truncated]"


def run_sql(w: WorkspaceClient, sql: str, *, wh: str | None = None) -> SqlOutcome:
    """Execute one statement and capture success rows or the refusal."""
    resp = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=wh or warehouse_id(w),
        wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
    )
    status = resp.status
    if status and status.state == StatementState.SUCCEEDED:
        rows = []
        if resp.result and resp.result.data_array:
            rows = [list(r) for r in resp.result.data_array]
        return SqlOutcome(True, rows=rows)

    err = status.error if status else None
    return SqlOutcome(
        False,
        error=err.message if err and err.message else str(status),
        error_code=str(err.error_code) if err and err.error_code else None,
    )


def run_sql_or_die(w: WorkspaceClient, sql: str, *, wh: str | None = None) -> SqlOutcome:
    outcome = run_sql(w, sql, wh=wh)
    if not outcome.succeeded:
        fail(f"setup statement failed: {outcome.error_code}: {outcome.error}")
        info(f"statement: {_truncate(sql, 300)}")
        sys.exit(1)
    return outcome


# --------------------------------------------------------------------------
# results
# --------------------------------------------------------------------------

VALID_STATUS = {"pass", "fail", "partial", "inconclusive"}

# results/ is committed to a public repo, and the most useful evidence — raw
# platform error strings — is exactly where real coordinates hide. Redaction
# happens at the write boundary so no individual script can forget it.
PLACEHOLDER_CATALOG = "my_catalog"
PLACEHOLDER_HOST = "https://my-workspace.cloud.databricks.com"
PLACEHOLDER_AUTHOR = "author@example.com"
PLACEHOLDER_CALLER = "00000000-0000-0000-0000-000000000000"


def _redaction_map() -> dict[str, str]:
    mapping = {CATALOG: PLACEHOLDER_CATALOG}
    caller_id = os.environ.get("EXPERIMENT_LOWPRIV_CLIENT_ID")
    if caller_id:
        mapping[caller_id] = PLACEHOLDER_CALLER
    try:
        cfg = admin_client().config
        if cfg.host:
            mapping[cfg.host.rstrip("/")] = PLACEHOLDER_HOST
        # bare hostname, for error strings that omit the scheme
        if cfg.hostname:
            mapping[cfg.hostname] = PLACEHOLDER_HOST.removeprefix("https://")
    except Exception:  # pragma: no cover - redaction must never break a run
        pass
    return mapping


_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def redact(blob: str) -> str:
    for real, placeholder in _redaction_map().items():
        if real:
            blob = blob.replace(real, placeholder)
    return _EMAIL_RE.sub(PLACEHOLDER_AUTHOR, blob)


def write_result(
    row_id: str,
    *,
    question: str,
    status: str,
    finding: str,
    evidence: dict[str, Any],
) -> None:
    """Append/overwrite one matrix row in results/matrix_results.json.

    ``status`` maps to the README matrix vocabulary:
      pass -> works as claimed, fail -> does not, partial -> partially,
      inconclusive -> could not be isolated (``finding`` must say why).
    """
    if status not in VALID_STATUS:
        raise ValueError(f"status must be one of {sorted(VALID_STATUS)}, got {status!r}")

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if RESULTS_PATH.exists():
        payload = json.loads(RESULTS_PATH.read_text())
    else:
        payload = {"rows": {}}

    payload["rows"][row_id] = json.loads(
        redact(
            json.dumps(
                {
                    "question": question,
                    "status": status,
                    "finding": finding,
                    "evidence": evidence,
                    "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                }
            )
        )
    )
    payload["environment"] = environment_shape()
    RESULTS_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    ok(f"recorded row {row_id}: {status}")


def environment_shape() -> dict[str, str]:
    """Results are claims about a moment — record the shape of the moment.

    Deliberately records no workspace host, account id, or user: this repo is
    public.
    """
    return {
        "cloud": "AWS",
        "workspace": "one real Databricks workspace (host redacted)",
        "compute": "serverless SQL warehouse (PRO channel)",
        "unity_catalog": "enabled",
        "caller_identity": "OAuth M2M service principal, workspace user (non-admin)",
        "author_identity": "workspace admin",
    }
