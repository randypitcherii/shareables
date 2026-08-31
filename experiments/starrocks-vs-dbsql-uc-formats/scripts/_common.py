"""Shared config, engine clients, timing, cost model, and results recording.

Config precedence: real environment variables win; otherwise ${APP_ENV:-dev}.env
is loaded from the experiment root (template.env documents the expected keys).

Fail-closed rules baked in here:
- record_result refuses to write a row that carries no proven identity.
- SQL errors from probes are returned as data (error text), never silently retried.
- Timings are client wall-clock from the operator machine for BOTH engines, so
  network overhead is comparable; the API-vs-wire-protocol difference is a
  documented caveat, not hidden.
"""

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

EXPERIMENT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_PATH = EXPERIMENT_ROOT / "results" / "matrix_results.json"

# Databricks SQL warehouse size -> DBU/hr for a single cluster (AWS list ratings).
WAREHOUSE_DBU_PER_HOUR = {
    "2X-Small": 4,
    "X-Small": 6,
    "Small": 12,
    "Medium": 24,
    "Large": 40,
    "X-Large": 80,
    "2X-Large": 144,
    "3X-Large": 272,
    "4X-Large": 528,
}


def _load_env_file() -> None:
    env_name = os.environ.get("APP_ENV", "dev")
    env_file = EXPERIMENT_ROOT / f"{env_name}.env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


@dataclass(frozen=True)
class Config:
    databricks_profile: str
    uc_catalog: str
    uc_schema: str
    warehouse_id: str
    starrocks_host: str
    starrocks_port: int
    starrocks_user: str
    starrocks_password: str
    row_count: int
    agg_runs: int
    ec2_usd_per_hour: float
    usd_per_dbu: float
    warehouse_dbu_per_hour: float


def load_config() -> Config:
    _load_env_file()
    required = ["UC_CATALOG", "DATABRICKS_WAREHOUSE_ID"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"Missing required config: {missing}. Copy template.env to dev.env.")
    return Config(
        databricks_profile=os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT"),
        uc_catalog=os.environ["UC_CATALOG"],
        uc_schema=os.environ.get("UC_SCHEMA", "starrocks_uc_eval"),
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        starrocks_host=os.environ.get("STARROCKS_HOST", ""),
        starrocks_port=int(os.environ.get("STARROCKS_PORT", "9030")),
        starrocks_user=os.environ.get("STARROCKS_USER", "root"),
        starrocks_password=os.environ.get("STARROCKS_PASSWORD", ""),
        row_count=int(os.environ.get("ROW_COUNT", "100000")),
        agg_runs=int(os.environ.get("AGG_RUNS", "7")),
        ec2_usd_per_hour=float(os.environ.get("STARROCKS_EC2_USD_PER_HOUR", "0.384")),
        usd_per_dbu=float(os.environ.get("DBSQL_USD_PER_DBU", "0.70")),
        warehouse_dbu_per_hour=float(os.environ.get("DBSQL_WAREHOUSE_DBU_PER_HOUR", "0")),
    )


# --- Databricks ---------------------------------------------------------------


def databricks_config(cfg: Config):
    """Databricks SDK Config for the named profile (OAuth, no PATs)."""
    from databricks.sdk.core import Config as SdkConfig

    return SdkConfig(profile=cfg.databricks_profile)


def oauth_token(cfg: Config) -> str:
    return databricks_config(cfg).oauth_token().access_token


def workspace_client(cfg: Config):
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(config=databricks_config(cfg))


class DbsqlError(RuntimeError):
    pass


def dbsql_exec(cfg: Config, statement: str, w=None) -> tuple[list[list], float]:
    """Run one statement on the serverless warehouse; return (rows, elapsed_seconds).

    Wall-clock timed from submit to terminal state. wait_timeout keeps the call
    synchronous for up to 50s; longer statements are polled. Errors raise
    DbsqlError carrying the server message — callers that probe support catch it
    and record the message as the finding.
    """
    w = w or workspace_client(cfg)
    started = time.perf_counter()
    resp = w.statement_execution.execute_statement(
        warehouse_id=cfg.warehouse_id,
        statement=statement,
        catalog=cfg.uc_catalog,
        schema=cfg.uc_schema,
        wait_timeout="50s",
    )
    while resp.status and resp.status.state and resp.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1.0)
        resp = w.statement_execution.get_statement(resp.statement_id)
    elapsed = time.perf_counter() - started
    state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    if state != "SUCCEEDED":
        message = resp.status.error.message if resp.status and resp.status.error else state
        raise DbsqlError(message)
    rows = resp.result.data_array if resp.result and resp.result.data_array else []
    return rows, elapsed


def warehouse_dbu_per_hour(cfg: Config, w=None) -> tuple[float, str]:
    """(DBU/hr, size-name) for the configured warehouse — env override wins."""
    w = w or workspace_client(cfg)
    wh = w.warehouses.get(cfg.warehouse_id)
    size = wh.cluster_size or "unknown"
    if cfg.warehouse_dbu_per_hour > 0:
        return cfg.warehouse_dbu_per_hour, size
    if size not in WAREHOUSE_DBU_PER_HOUR:
        raise SystemExit(
            f"Unknown warehouse size {size!r}; set DBSQL_WAREHOUSE_DBU_PER_HOUR explicitly."
        )
    return float(WAREHOUSE_DBU_PER_HOUR[size]), size


# --- StarRocks ----------------------------------------------------------------


def starrocks_conn(cfg: Config):
    import pymysql

    if not cfg.starrocks_host:
        raise SystemExit("STARROCKS_HOST is not set — run `make tf-apply` and update dev.env.")
    return pymysql.connect(
        host=cfg.starrocks_host,
        port=cfg.starrocks_port,
        user=cfg.starrocks_user,
        password=cfg.starrocks_password,
        autocommit=True,
        read_timeout=600,
        write_timeout=600,
    )


def sr_exec(conn, statement: str) -> tuple[list[tuple], float]:
    """Run one statement over the MySQL protocol; return (rows, elapsed_seconds)."""
    started = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(statement)
        rows = list(cur.fetchall()) if cur.description else []
    return rows, time.perf_counter() - started


# --- Cost model ---------------------------------------------------------------


def starrocks_cost_usd(cfg: Config, elapsed_s: float) -> float:
    return round(cfg.ec2_usd_per_hour * elapsed_s / 3600, 6)


def dbsql_cost_usd(dbu_per_hour: float, usd_per_dbu: float, elapsed_s: float) -> float:
    return round(dbu_per_hour * usd_per_dbu * elapsed_s / 3600, 6)


# --- Timing / stats -----------------------------------------------------------


def timing_stats(elapsed_list: list[float]) -> dict:
    """Summarize repeated-run wall-clock timings (seconds in, ms out)."""
    if not elapsed_list:
        return {}
    ms = sorted(round(e * 1000) for e in elapsed_list)
    return {
        "runs": len(ms),
        "first_run_ms": round(elapsed_list[0] * 1000),
        "p50_ms": ms[len(ms) // 2],
        "min_ms": ms[0],
        "max_ms": ms[-1],
        "all_ms": [round(e * 1000) for e in elapsed_list],
    }


# --- Shared SQL (both dialects) -----------------------------------------------

EVENT_COLUMNS = "(seq, event_id, event_ts, device_id, region, value, payload)"

REGION_CASE = (
    "CASE CAST({id} % 5 AS INT) WHEN 0 THEN 'us-east' WHEN 1 THEN 'us-west' "
    "WHEN 2 THEN 'eu-west' WHEN 3 THEN 'ap-south' ELSE 'sa-east' END"
)


def _gen_exprs(id_expr: str, str_cast: str) -> str:
    """Deterministic column expressions shared by both dialects.

    Integer-derived values only, so both engines generate bit-identical data and
    the cross-engine checksum can demand exact agreement.
    """
    return (
        f"{id_expr} AS seq, "
        f"lpad(CAST({id_expr} AS {str_cast}), 12, '0') AS event_id, "
        f"timestampadd(SECOND, CAST({id_expr} % 86400 AS INT), "
        f"CAST('2026-01-01 00:00:00' AS {{ts_type}}) ) AS event_ts, "
        f"concat('dev-', lpad(CAST({id_expr} % 5000 AS {str_cast}), 5, '0')) AS device_id, "
        f"{REGION_CASE.format(id=id_expr)} AS region, "
        f"CAST({id_expr} % 100000 AS DOUBLE) / 100 AS value, "
        f"repeat('x', 200) AS payload"
    )


def gen_select_dbsql(n: int) -> str:
    exprs = _gen_exprs("id", "STRING").format(ts_type="TIMESTAMP_NTZ")
    return f"SELECT {exprs} FROM range(1, {n + 1})"


def gen_select_starrocks(n: int) -> str:
    exprs = _gen_exprs("g.generate_series", "VARCHAR").format(ts_type="DATETIME")
    return (
        f"SELECT {exprs} FROM TABLE(generate_series(1, {n + 1})) g "
        f"WHERE g.generate_series <= {n}"
    )


SINGLE_ROW_SEQ = 100_000_001

SINGLE_ROW_VALUES = (
    f"({SINGLE_ROW_SEQ}, 'single-row-probe', '2026-01-02 00:00:00', "
    "'dev-single', 'us-east', 42.42, 'single')"
)

CHECKSUM_SQL = (
    "SELECT COUNT(*), SUM(seq), SUM(CAST(round(value * 100) AS BIGINT)), "
    "COUNT(DISTINCT device_id) FROM {table}"
)

AGG_QUERIES = {
    "agg_group_by": "SELECT region, COUNT(*) AS c, SUM(value) AS s FROM {table} GROUP BY region",
    "agg_filter": (
        "SELECT device_id, COUNT(*) AS c, AVG(value) AS a FROM {table} "
        "WHERE value > 500 GROUP BY device_id ORDER BY c DESC, device_id LIMIT 10"
    ),
    "agg_distinct": "SELECT COUNT(DISTINCT event_id) FROM {table}",
}


# --- Results ------------------------------------------------------------------


def record_result(row_key: str, payload: dict) -> None:
    """Merge one matrix row into results/matrix_results.json.

    Fail closed: refuses rows that carry no proven engine identity.

    Re-running a cell in a different environment must not erase what the first
    attempt found. Set RESULT_KEY_SUFFIX to write the rerun under its own key
    (the original row stays intact), and RESULT_RUN_NOTE to say on the row
    itself which environment produced it.
    """
    if not payload.get("identity"):
        raise SystemExit(f"refusing to record {row_key}: no proven identity in payload")
    row_key = f"{row_key}{os.environ.get('RESULT_KEY_SUFFIX', '')}"
    note = os.environ.get("RESULT_RUN_NOTE")
    if note:
        payload = payload | {"run_note": note}
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if RESULTS_PATH.exists():
        existing = json.loads(RESULTS_PATH.read_text())
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    existing[row_key] = payload | {"recorded_at": stamp}
    RESULTS_PATH.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n")
    print(f"recorded {row_key} -> {RESULTS_PATH}")


def section(title: str) -> None:
    print(f"\n=== {title} ===")
