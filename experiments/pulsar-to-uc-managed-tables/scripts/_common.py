"""Shared config, Databricks helpers, and results recording for the evaluation scripts.

Config precedence: real environment variables win; otherwise ${APP_ENV:-dev}.env is
loaded from the experiment root (template.env documents the expected keys).
"""

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

EXPERIMENT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_PATH = EXPERIMENT_ROOT / "results" / "matrix_results.json"


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
    pulsar_service_url: str
    pulsar_admin_url: str
    kafka_bootstrap: str
    pulsar_topic: str
    databricks_profile: str
    uc_catalog: str
    uc_schema: str
    warehouse_id: str
    event_count: int
    event_rate_per_sec: int
    event_payload_bytes: int


def load_config() -> Config:
    _load_env_file()
    required = ["PULSAR_SERVICE_URL", "UC_CATALOG"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"Missing required config: {missing}. Copy template.env to dev.env.")
    return Config(
        pulsar_service_url=os.environ["PULSAR_SERVICE_URL"],
        pulsar_admin_url=os.environ.get("PULSAR_ADMIN_URL", ""),
        kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", ""),
        pulsar_topic=os.environ.get("PULSAR_TOPIC", "persistent://public/default/uc-ingest-eval"),
        databricks_profile=os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT"),
        uc_catalog=os.environ["UC_CATALOG"],
        uc_schema=os.environ.get("UC_SCHEMA", "pulsar_uc_ingest_eval"),
        warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID", ""),
        event_count=int(os.environ.get("EVENT_COUNT", "100000")),
        event_rate_per_sec=int(os.environ.get("EVENT_RATE_PER_SEC", "5000")),
        event_payload_bytes=int(os.environ.get("EVENT_PAYLOAD_BYTES", "512")),
    )


def databricks_config(cfg: Config):
    """Databricks SDK Config for the named profile (OAuth, no PATs)."""
    from databricks.sdk.core import Config as SdkConfig

    return SdkConfig(profile=cfg.databricks_profile)


def oauth_token(cfg: Config) -> str:
    return databricks_config(cfg).oauth_token().access_token


def workspace_client(cfg: Config):
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(config=databricks_config(cfg))


def run_sql(cfg: Config, statement: str) -> list[list]:
    """Execute a statement on the configured SQL warehouse and return data rows."""
    w = workspace_client(cfg)
    resp = w.statement_execution.execute_statement(
        warehouse_id=cfg.warehouse_id,
        statement=statement,
        catalog=cfg.uc_catalog,
        schema=cfg.uc_schema,
        wait_timeout="50s",
    )
    if resp.status and resp.status.state and resp.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"SQL failed ({resp.status.state}): {resp.status.error}")
    return resp.result.data_array if resp.result and resp.result.data_array else []


def record_result(path_key: str, payload: dict) -> None:
    """Merge one path's result into results/matrix_results.json."""
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if RESULTS_PATH.exists():
        existing = json.loads(RESULTS_PATH.read_text())
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    existing[path_key] = payload | {"recorded_at": stamp}
    RESULTS_PATH.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n")
    print(f"recorded {path_key} -> {RESULTS_PATH}")


def latency_stats_ms(latencies: list[float]) -> dict:
    if not latencies:
        return {}
    s = sorted(latencies)

    def pct(p: float) -> float:
        return s[min(len(s) - 1, int(p * len(s)))]

    return {
        "count": len(s),
        "p50_ms": round(pct(0.50)),
        "p95_ms": round(pct(0.95)),
        "p99_ms": round(pct(0.99)),
        "max_ms": round(s[-1]),
    }
