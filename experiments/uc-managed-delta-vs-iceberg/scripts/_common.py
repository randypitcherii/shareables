"""Shared live-SQL, external-client, and evidence helpers for the Delta-vs-Iceberg experiment."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results" / "matrix_results.json"
PROFILE = os.getenv("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
WAREHOUSE_ID = os.getenv("EXPERIMENT_WAREHOUSE_ID", "")
STORAGE_ROOT = os.getenv("EXPERIMENT_STORAGE_ROOT", "").rstrip("/")
CATALOG = os.getenv("EXPERIMENT_CATALOG", "uc_delta_vs_iceberg_experiment")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SCHEMA = "matrix"
FQ = f"`{CATALOG}`.`{SCHEMA}`"
VOLUME = "evidence"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
USER_AGENT = "shareables-uc-delta-vs-iceberg/0.1"

# The two formats under test. Both are UC managed tables clustered with Liquid on `cat`.
DELTA_PROPS = {
    "delta.feature.catalogManaged": "supported",  # catalog commits (external Delta writes)
    "delta.enableIcebergCompatV3": "true",  # Iceberg v3 reads (UniForm)
    "delta.universalFormat.enabledFormats": "iceberg",
    "delta.enableRowTracking": "true",  # required for automatic CDF on Delta
}
FORMATS = {
    "delta": "Managed Delta (Liquid, catalog commits, Iceberg v3 reads, row tracking)",
    "iceberg": "Managed Iceberg v3 (Liquid)",
}
VALID_STATUSES = {"pass", "fail", "partial", "inconclusive", "not_applicable"}


def create_table_sql(name: str, fmt: str) -> str:
    cols = "(id BIGINT, cat STRING, v BIGINT)"
    if fmt == "delta":
        props = ", ".join(f"'{k}' = '{v}'" for k, v in DELTA_PROPS.items())
        return f"CREATE TABLE {FQ}.{name} {cols} CLUSTER BY (cat) TBLPROPERTIES ({props})"
    if fmt == "iceberg":
        return (
            f"CREATE TABLE {FQ}.{name} {cols} USING ICEBERG CLUSTER BY (cat) "
            "TBLPROPERTIES ('format-version' = '3')"
        )
    if fmt == "iceberg_v2":
        return (
            f"CREATE TABLE {FQ}.{name} {cols} USING ICEBERG CLUSTER BY (cat) "
            # Liquid on v2 requires turning off DVs + row tracking (reduced concurrency control):
            # MANAGED_ICEBERG_ATTEMPTED_TO_ENABLE_CLUSTERING_WITHOUT_DISABLING_DVS_OR_ROW_TRACKING
            "TBLPROPERTIES ('format-version' = '2', 'iceberg.enableDeletionVectors' = 'false', "
            "'iceberg.enableRowTracking' = 'false')"
        )
    raise ValueError(fmt)


@dataclass
class SqlResult:
    succeeded: bool
    rows: list[list[str | None]]
    error: str | None = None
    error_code: str | None = None

    @property
    def scalar(self) -> str | None:
        return self.rows[0][0] if self.rows else None


@cache
def client() -> WorkspaceClient:
    if os.getenv("DATABRICKS_TOKEN", "").startswith("dapi"):
        raise SystemExit("Personal access tokens are refused; use an OAuth/SSO CLI profile.")
    w = WorkspaceClient(profile=PROFILE)
    if w.config.auth_type not in {"databricks-cli", "oauth-m2m", "external-browser"}:
        raise SystemExit(f"Refusing auth_type={w.config.auth_type}; use an OAuth/SSO profile.")
    return w


def host() -> str:
    return client().config.host.rstrip("/")


def oauth_token() -> str:
    """Short-lived OAuth access token from the CLI profile (never a PAT)."""
    token = client().config.authenticate()["Authorization"].removeprefix("Bearer ")
    if token.startswith("dapi"):
        raise SystemExit("Profile resolved to a PAT; refusing.")
    return token


def sql(statement: str) -> SqlResult:
    if not WAREHOUSE_ID:
        raise SystemExit("EXPERIMENT_WAREHOUSE_ID is required")
    try:
        response = client().statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=statement,
            wait_timeout="50s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        )
        while response.status and response.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            response = client().statement_execution.get_statement(response.statement_id)
        if not response.status or response.status.state != StatementState.SUCCEEDED:
            error = response.status.error if response.status else None
            return SqlResult(
                False,
                [],
                scrub(str(error.message if error else response.status)),
                str(error.error_code if error else "UNKNOWN"),
            )
        rows = response.result.data_array if response.result and response.result.data_array else []
        return SqlResult(True, rows)
    except Exception as exc:  # noqa: BLE001 -- SDK errors are experiment evidence
        return SqlResult(False, [], scrub(str(exc)), type(exc).__name__)


def must_sql(statement: str) -> SqlResult:
    result = sql(statement)
    if not result.succeeded:
        raise SystemExit(f"SQL failed: {result.error_code}: {result.error}\n{statement}")
    return result


def rows_of(table: str) -> list[list[Any]]:
    return must_sql(f"SELECT id, cat, v FROM {FQ}.{table} ORDER BY id").rows


# ---------------------------------------------------------------- external clients


def duckdb_connect():
    import duckdb

    return duckdb.connect()


def duckdb_attach_iceberg(con, alias: str = "ice") -> None:
    con.execute("INSTALL iceberg; LOAD iceberg")
    con.execute(f"CREATE OR REPLACE SECRET ice_secret (TYPE iceberg, TOKEN '{oauth_token()}')")
    con.execute(
        f"ATTACH '{CATALOG}' AS {alias} (TYPE iceberg, SECRET ice_secret, "
        f"ENDPOINT '{host()}/api/2.1/unity-catalog/iceberg-rest')"
    )


def duckdb_attach_uc(con, alias: str = "uc", read_only: bool = False) -> None:
    con.execute("INSTALL delta; LOAD delta; INSTALL unity_catalog; LOAD unity_catalog")
    con.execute(
        f"CREATE OR REPLACE SECRET uc_secret (TYPE unity_catalog, TOKEN '{oauth_token()}', "
        f"ENDPOINT '{host()}', AWS_REGION '{AWS_REGION}')"
    )
    opts = f"TYPE unity_catalog, SECRET uc_secret, DEFAULT_SCHEMA '{SCHEMA}'"
    if read_only:
        opts += ", READ_ONLY"
    con.execute(f"ATTACH '{CATALOG}' AS {alias} ({opts})")


def attempt(fn) -> dict[str, Any]:
    """Run one client operation; failures are evidence, never exceptions."""
    try:
        value = fn()
        return {"ok": True, "value": scrub_value(value)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error_class": type(exc).__name__, "error": scrub(str(exc))}


# ---------------------------------------------------------------- scrubbing + results

_SCRUBS: list[tuple[str, str]] = [
    (r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "author@example.com"),
    (r"https://[^\s,\"'}\]]+", "https://workspace.example.com"),
    (r"s3a?://[^\s,\"'}\]]+", "s3://experiment-bucket/<path>"),
    (r"arn:aws:[^\s,\"'}\]]+", "arn:aws:<redacted>"),
    (r"\bm=[\w-]+,s=[\w-]+", "<sts-session>"),
    (r"Request ID: [A-Z0-9]+", "Request ID: <id>"),
    (r"bucket '[^']*'?", "bucket 'experiment-bucket'"),
    (r",s=[\w-]{16,}", ",<sts-session>"),
    (r"\b\d{12}\b", "<aws-account>"),
    (r"(?i)request[_ ]?id[=:]\s*\"?[0-9a-f-]{36}\"?", "request_id=<id>"),
    (r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", "<uuid>"),
    (r"/Workspace/Users/[^\s/\"']+", "/Workspace/Users/<user>"),
    (r"\beyJ[\w-]+\.[\w-]+\.[\w-]+", "<jwt>"),
    (r"\b[0-9a-f]{16}\b", "<id16>"),
]


def scrub(value: str) -> str:
    if STORAGE_ROOT:
        value = value.replace(STORAGE_ROOT, "s3://experiment-bucket/<path>")
        bucket = STORAGE_ROOT.removeprefix("s3://").split("/", 1)[0]
        value = value.replace(bucket, "experiment-bucket")
        value = value.replace(re.sub(r"\b\d{12}\b", "<aws-account>", bucket), "experiment-bucket")
    for pattern, repl in _SCRUBS:
        value = re.sub(pattern, repl, value)
    return value[:2000] + ("...[truncated]" if len(value) > 2000 else "")


def scrub_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: scrub_value(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [scrub_value(v) for v in value]
    if isinstance(value, str):
        return scrub(value)
    return value


def environment() -> dict[str, Any]:
    return {
        "cloud": "AWS",
        "workspace": "one real Databricks workspace (host redacted)",
        "principal": "one workspace user via OAuth U2M (identity redacted)",
        "sql": "serverless SQL warehouse (DBSQL version recorded per row where relevant)",
        "spark": "classic single-node job cluster pinned to DBR 19.x (version recorded per row)",
        "external_clients": "DuckDB (iceberg + unity_catalog extensions), PyIceberg",
        "tables": list(FORMATS.values()),
    }


def write_result(
    row: str, *, question: str, cells: dict[str, dict[str, Any]], evidence: dict[str, Any]
) -> None:
    """cells: {format: {"status": ..., "finding": ...}}."""
    for fmt, cell in cells.items():
        if cell["status"] not in VALID_STATUSES:
            raise ValueError(f"invalid status for {row}/{fmt}: {cell['status']}")
    payload = (
        json.loads(RESULTS.read_text()) if RESULTS.exists() else {"environment": {}, "rows": {}}
    )
    payload["environment"] = environment()
    payload["rows"][row] = {
        "question": question,
        "cells": scrub_value(cells),
        "evidence": scrub_value(evidence),
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    RESULTS.parent.mkdir(exist_ok=True)
    RESULTS.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")
    print(f"recorded row {row}: " + ", ".join(f"{k}={v['status']}" for k, v in cells.items()))


SPARK_RAW = ROOT / "results" / ".spark_raw.json"


def spark_raw() -> dict[str, Any]:
    """Raw evidence from `make spark` (scripts/spark/matrix_job.py)."""
    if not SPARK_RAW.exists():
        raise SystemExit("results/.spark_raw.json missing -- run `make spark` first")
    return json.loads(SPARK_RAW.read_text())


def short(cell: dict[str, Any], n: int = 240) -> str:
    if cell.get("ok"):
        return "ok"
    return scrub(str(cell.get("error", ""))).replace("\n", " ")[:n]
