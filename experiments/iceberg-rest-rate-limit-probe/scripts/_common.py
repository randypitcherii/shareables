"""Shared config, auth, SQL, and scrubbing helpers for the probe scripts.

Config precedence: real environment variables win; otherwise ${APP_ENV:-dev}.env is
loaded from the experiment root (template.env documents the keys).
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

EXPERIMENT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = EXPERIMENT_ROOT / "data"  # gitignored: raw per-call logs
RESULTS_DIR = EXPERIMENT_ROOT / "results"  # committed: scrubbed aggregates only

IRC_PATH = "/api/2.1/unity-catalog/iceberg-rest"
LEGACY_PATH = "/api/2.1/unity-catalog/iceberg"


def _load_env_file() -> None:
    env_file = EXPERIMENT_ROOT / f"{os.environ.get('APP_ENV', 'dev')}.env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.split(" #", 1)[0].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


@dataclass(frozen=True)
class Config:
    profile_a: str
    profile_b: str
    cloud: str
    region: str
    uc_catalog: str
    uc_catalog_b: str
    uc_schema: str
    table_count: int
    warehouse_id: str
    warehouse_id_b: str
    max_rate: float
    max_duration_sec: float
    max_concurrency: int
    run_id: str
    profiles: dict = field(default_factory=dict)

    def profile(self, label: str) -> str:
        p = {"a": self.profile_a, "b": self.profile_b}[label]
        if not p:
            raise SystemExit(f"DATABRICKS_PROFILE_{label.upper()} is not set (see template.env)")
        return p

    def labels(self) -> list[str]:
        return ["a", "b"] if self.profile_b else ["a"]

    def catalog(self, label: str) -> str:
        """Each workspace probes its own catalog (both live on the same metastore)."""
        return (self.uc_catalog_b if label == "b" else "") or self.uc_catalog

    def warehouse(self, label: str) -> str:
        return (self.warehouse_id_b if label == "b" else "") or self.warehouse_id

    def table_names(self) -> list[str]:
        return [f"t{i:03d}" for i in range(self.table_count)]


def load_config() -> Config:
    _load_env_file()
    env = os.environ.get
    return Config(
        profile_a=env("DATABRICKS_PROFILE_A", ""),
        profile_b=env("DATABRICKS_PROFILE_B", ""),
        cloud=env("PROBE_CLOUD", "unknown"),
        region=env("PROBE_REGION", "unknown"),
        uc_catalog=env("UC_CATALOG", ""),
        uc_catalog_b=env("UC_CATALOG_B", ""),
        uc_schema=env("UC_SCHEMA", "iceberg_rest_probe"),
        table_count=int(env("PROBE_TABLE_COUNT", "50")),
        warehouse_id=env("DATABRICKS_WAREHOUSE_ID", ""),
        warehouse_id_b=env("DATABRICKS_WAREHOUSE_ID_B", ""),
        max_rate=float(env("PROBE_MAX_RATE", "1000")),
        max_duration_sec=float(env("PROBE_MAX_DURATION_SEC", "900")),
        max_concurrency=int(env("PROBE_MAX_CONCURRENCY", "400")),
        run_id=env("RUN_ID", "dev-run"),
    )


def workspace_client(profile: str):
    """Profile forms:
    - `<name>`: a CLI profile (laptop runs)
    - `runtime`: the ambient identity of a Databricks job/notebook
    - `sp:<scope>`: inside a job, a service principal whose OAuth client_id / client_secret
      sit in secret scope <scope> (a second identity for the per-principal scope test)
    """
    from databricks.sdk import WorkspaceClient

    if profile == "runtime":
        return WorkspaceClient()
    if profile.startswith("sp:"):
        import base64

        rt = WorkspaceClient()
        scope = profile.split(":", 1)[1]

        def secret(key: str) -> str:
            return base64.b64decode(rt.secrets.get_secret(scope, key).value).decode()

        return WorkspaceClient(host=rt.config.host, client_id=secret("client_id"),
                               client_secret=secret("client_secret"), auth_type="oauth-m2m")
    return WorkspaceClient(profile=profile)


def host_and_headers(profile: str) -> tuple[str, dict[str, str]]:
    """Workspace host plus an Authorization header minted by the SDK for `profile`.

    `mock=<url>` points at the local token-bucket mock (tests/mock_irc.py) instead.
    """
    if profile.startswith("mock="):
        return profile.split("=", 1)[1].rstrip("/"), {"Authorization": "Bearer mock"}
    w = workspace_client(profile)
    return w.config.host.rstrip("/"), w.config.authenticate()


def run_sql(cfg: Config, label: str, statement: str, max_polls: int = 60) -> list[list]:
    """Execute one statement on workspace `label`'s SQL warehouse and return its rows."""
    w = workspace_client(cfg.profile(label))
    resp = w.statement_execution.execute_statement(
        warehouse_id=cfg.warehouse(label), statement=statement, wait_timeout="50s"
    )
    polls = 0
    while resp.status.state.value in ("PENDING", "RUNNING"):
        if polls >= max_polls:
            raise TimeoutError(f"statement still {resp.status.state} after polling")
        time.sleep(5)
        polls += 1
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"SQL failed ({resp.status.state}): {resp.status.error}")
    return resp.result.data_array if resp.result and resp.result.data_array else []


# --- scrubbing --------------------------------------------------------------------

_SCRUB_PATTERNS = [
    (re.compile(r"https?://[A-Za-z0-9.-]+\.(?:databricks\.com|azuredatabricks\.net)\S*"), "<host>"),
    (re.compile(r"[A-Za-z0-9-]+\.cloud\.databricks\.com"), "<host>"),
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"), "<uuid>"),
    (re.compile(r"\b\d{13,19}\b"), "<id>"),  # workspace / org IDs
    (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "<email>"),
    (re.compile(r"\bdapi[0-9a-f]{20,}\b"), "<token>"),
]


def scrub(text: str | None, extra: tuple[str, ...] = ()) -> str | None:
    """Remove hostnames, UUIDs, numeric IDs, emails, tokens and any `extra` literals."""
    if text is None:
        return None
    for literal in extra:
        if literal and len(literal) >= 3:
            text = re.sub(rf"(?<![\w-]){re.escape(literal)}(?![\w-])", "<redacted>", text)
    for pattern, repl in _SCRUB_PATTERNS:
        text = pattern.sub(repl, text)
    return text


def stamp(cfg: Config, endpoint: str) -> dict:
    """Environment stamp attached to every committed result."""
    return {
        "cloud": cfg.cloud,
        "region": cfg.region,
        "date_utc": time.strftime("%Y-%m-%d", time.gmtime()),
        "endpoint": endpoint,
    }


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
