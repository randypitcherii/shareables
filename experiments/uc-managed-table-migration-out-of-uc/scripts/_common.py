"""Shared live-SQL and evidence helpers for the managed-table exit experiment."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results" / "matrix_results.json"
PROFILE = os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
CATALOG = os.getenv("EXPERIMENT_CATALOG", "my_catalog")
SCHEMA = os.getenv("EXPERIMENT_SCHEMA", "uc_managed_exit_experiment")
WAREHOUSE_ID = os.getenv("EXPERIMENT_WAREHOUSE_ID", "")
EXTERNAL_ROOT = os.getenv("EXPERIMENT_EXTERNAL_ROOT", "").rstrip("/")
AWS_PROFILE = os.getenv("AWS_PROFILE", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
GLUE_DATABASE = os.getenv("EXPERIMENT_GLUE_DATABASE", "uc_managed_exit_experiment")
ATHENA_OUTPUT = os.getenv("EXPERIMENT_ATHENA_OUTPUT", "")
MANAGED_CATALOG = os.getenv("EXPERIMENT_MANAGED_CATALOG", "uc_managed_exit_customer_storage")
MANAGED_STORAGE_ROOT = os.getenv("EXPERIMENT_MANAGED_STORAGE_ROOT", "").rstrip("/")
FQ_SCHEMA = f"`{CATALOG}`.`{SCHEMA}`"
VALID_STATUSES = {"pass", "fail", "partial", "inconclusive"}


@dataclass
class SqlResult:
    succeeded: bool
    rows: list[list[str | None]]
    error: str | None = None
    error_code: str | None = None

    @property
    def scalar(self) -> str | None:
        return self.rows[0][0] if self.rows else None


def client() -> WorkspaceClient:
    token = os.getenv("DATABRICKS_TOKEN", "")
    if token.startswith("dapi"):
        raise SystemExit("Personal access tokens are refused; use an OAuth/SSO CLI profile.")
    if not WAREHOUSE_ID:
        raise SystemExit("EXPERIMENT_WAREHOUSE_ID is required")
    return WorkspaceClient(profile=PROFILE)


def sql(statement: str) -> SqlResult:
    try:
        response = client().statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=statement,
            wait_timeout="50s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
        )
        if not response.status or response.status.state != StatementState.SUCCEEDED:
            error = response.status.error if response.status else None
            return SqlResult(
                False,
                [],
                str(error.message if error else response.status),
                str(error.error_code if error else "UNKNOWN"),
            )
        rows = response.result.data_array if response.result and response.result.data_array else []
        return SqlResult(True, rows)
    except Exception as exc:  # noqa: BLE001 -- SDK errors are experiment evidence
        return SqlResult(False, [], _scrub(str(exc)), type(exc).__name__)


def aws(*args: str) -> subprocess.CompletedProcess[str]:
    command = ["aws", *args, "--profile", AWS_PROFILE, "--region", AWS_REGION]
    return subprocess.run(command, capture_output=True, check=False, text=True)


def clear_external_path(path: str) -> None:
    if not path.startswith(f"{EXTERNAL_ROOT}/"):
        raise ValueError("refusing to delete outside EXPERIMENT_EXTERNAL_ROOT")
    removed = aws("s3", "rm", path, "--recursive")
    if removed.returncode:
        raise SystemExit(f"Failed to clear experiment path: {_scrub(removed.stderr)}")


def athena(statement: str) -> SqlResult:
    started = aws(
        "athena",
        "start-query-execution",
        "--query-string",
        statement,
        "--query-execution-context",
        f"Database={GLUE_DATABASE}",
        "--result-configuration",
        f"OutputLocation={ATHENA_OUTPUT}",
        "--output",
        "json",
    )
    if started.returncode:
        return SqlResult(False, [], _scrub(started.stderr), "ATHENA_START_FAILED")
    query_id = json.loads(started.stdout)["QueryExecutionId"]
    for _ in range(90):
        polled = aws(
            "athena", "get-query-execution", "--query-execution-id", query_id, "--output", "json"
        )
        if polled.returncode:
            return SqlResult(False, [], _scrub(polled.stderr), "ATHENA_POLL_FAILED")
        status = json.loads(polled.stdout)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            fetched = aws(
                "athena", "get-query-results", "--query-execution-id", query_id, "--output", "json"
            )
            if fetched.returncode:
                return SqlResult(False, [], _scrub(fetched.stderr), "ATHENA_RESULTS_FAILED")
            raw_rows = json.loads(fetched.stdout)["ResultSet"].get("Rows", [])
            rows = [
                [column.get("VarCharValue") for column in row.get("Data", [])]
                for row in raw_rows[1:]
            ]
            return SqlResult(True, rows)
        if state in {"FAILED", "CANCELLED"}:
            reason = status.get("StateChangeReason", state)
            return SqlResult(False, [], _scrub(reason), f"ATHENA_{state}")
        time.sleep(2)
    return SqlResult(False, [], "Athena query timed out", "ATHENA_TIMEOUT")


def must_athena(statement: str) -> SqlResult:
    result = athena(statement)
    if not result.succeeded:
        raise SystemExit(f"Athena failed: {result.error_code}: {result.error}")
    return result


def must_sql(statement: str) -> SqlResult:
    result = sql(statement)
    if not result.succeeded:
        raise SystemExit(f"SQL failed: {result.error_code}: {result.error}")
    return result


def detail(table: str) -> dict[str, Any]:
    result = must_sql(f"DESCRIBE DETAIL {table}")
    keys = [
        "format",
        "id",
        "name",
        "description",
        "location",
        "createdAt",
        "lastModified",
        "partitionColumns",
        "clusteringColumns",
        "numFiles",
        "sizeInBytes",
        "properties",
        "minReaderVersion",
        "minWriterVersion",
        "tableFeatures",
        "statistics",
    ]
    return dict(zip(keys, result.rows[0], strict=False))


def count(table: str) -> int:
    return int(must_sql(f"SELECT count(*) FROM {table}").scalar or 0)


def _scrub(value: str) -> str:
    value = (
        value.replace(EXTERNAL_ROOT, "s3://experiment-bucket/uc-managed-exit")
        if EXTERNAL_ROOT
        else value
    )
    value = value.replace(CATALOG, "my_catalog").replace(SCHEMA, "uc_managed_exit_experiment")
    value = value.replace(MANAGED_CATALOG, "uc_managed_exit_customer_storage")
    value = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "author@example.com", value)
    value = re.sub(r"https://[^\s,\"}]+", "https://workspace.example.com", value)
    return value[:2000] + ("...[truncated]" if len(value) > 2000 else "")


def _scrub_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _scrub_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_scrub_value(item) for item in value]
    if isinstance(value, str):
        return _scrub(value)
    return value


def summary(result: SqlResult) -> dict[str, Any]:
    return {
        "succeeded": result.succeeded,
        "scalar": _scrub(str(result.scalar)) if result.scalar is not None else None,
        "error_code": result.error_code,
        "error": _scrub(result.error) if result.error else None,
    }


def write_result(
    row: str, *, question: str, status: str, finding: str, evidence: dict[str, Any]
) -> None:
    if status not in VALID_STATUSES:
        raise ValueError(f"invalid status: {status}")
    payload = (
        json.loads(RESULTS.read_text()) if RESULTS.exists() else {"environment": {}, "rows": {}}
    )
    payload["environment"] = {
        "cloud": "AWS",
        "workspace": "one real Databricks workspace (host redacted)",
        "compute": "serverless SQL warehouse",
        "source_tables": "UC managed Delta and managed Iceberg",
        "external_storage": "customer-owned object storage (path redacted)",
        "managed_storage_scenario": "isolated UC catalog rooted in the same customer-owned storage",
    }
    payload["rows"][row] = {
        "question": question,
        "status": status,
        "finding": _scrub(finding),
        "evidence": _scrub_value(evidence),
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    RESULTS.parent.mkdir(exist_ok=True)
    RESULTS.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
