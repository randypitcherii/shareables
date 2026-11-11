"""Shared helpers: config from env, Databricks client, SQL execution, results IO.

Every live claim in the findings matrix flows through this module, so auth and
evidence capture stay uniform across setup, the matrix runner, and teardown.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

EXPERIMENT_ROOT = Path(__file__).resolve().parent.parent
PROJECT_DIR = EXPERIMENT_ROOT / "dbt_project"
RESULTS_PATH = EXPERIMENT_ROOT / "results" / "matrix_results.json"
LANDING_FILES = EXPERIMENT_ROOT / "landing_files"


def _load_dotenv() -> None:
    env_path = EXPERIMENT_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


_load_dotenv()

PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
CATALOG = os.environ.get("EXPERIMENT_CATALOG", "my_catalog")
SCHEMA = os.environ.get("EXPERIMENT_SCHEMA", "dbt_v2_compat")
WAREHOUSE_ID = os.environ.get("EXPERIMENT_WAREHOUSE_ID", "")
VOLUME = "landing"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
FQ_SCHEMA = f"`{CATALOG}`.`{SCHEMA}`"


def client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(profile=PROFILE)


def warehouse_id(w) -> str:
    """The configured warehouse, else the first serverless one."""
    if WAREHOUSE_ID:
        return WAREHOUSE_ID
    for wh in w.warehouses.list():
        if wh.enable_serverless_compute:
            return wh.id
    raise SystemExit("No serverless SQL warehouse found; set EXPERIMENT_WAREHOUSE_ID.")


def sql(w, statement: str) -> list[list[Any]]:
    """Run one statement on the warehouse and return its rows (raises on failure)."""
    from databricks.sdk.service.sql import StatementState

    resp = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id(w), wait_timeout="50s"
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        err = resp.status.error.message if resp.status.error else resp.status.state
        raise RuntimeError(f"{err}\n  statement: {statement}")
    return (resp.result.data_array or []) if resp.result else []


def dbt_env(w) -> dict[str, str]:
    """Environment the committed profiles.yml reads, with a fresh bearer token."""
    wh = w.warehouses.get(warehouse_id(w))
    token = w.config.authenticate()["Authorization"].removeprefix("Bearer ")
    return {
        **os.environ,
        "DBT_HOST": w.config.host.removeprefix("https://"),
        "DBT_HTTP_PATH": wh.odbc_params.path,
        "DBT_CATALOG": CATALOG,
        "DBT_SCHEMA": SCHEMA,
        "DBT_LANDING_PATH": LANDING_PATH,
        "DBT_ENV_SECRET_DATABRICKS_TOKEN": token,
        "DATABRICKS_CONFIG_PROFILE": PROFILE,
        "DO_NOT_TRACK": "1",
    }


def now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def load_results() -> dict[str, Any]:
    if RESULTS_PATH.exists():
        return json.loads(RESULTS_PATH.read_text())
    return {"rows": {}}


def save_results(data: dict[str, Any]) -> None:
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
