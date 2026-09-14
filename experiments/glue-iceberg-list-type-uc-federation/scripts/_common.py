"""Shared config, clients, type-shape catalog, redaction, and results recording.

Config precedence: real environment variables win; otherwise ${APP_ENV:-dev}.env
is loaded from the experiment root (template.env documents the expected keys).

Fail-closed rules baked in here:
- record_result refuses a row that carries no proven identity for the reader
  that produced it (Databricks current_user() or AWS caller ARN).
- SQL / API errors from probes are returned as data, never retried or swallowed.
  The *error class* is the finding.
- Everything written to results/ passes through redact(): account id, bucket,
  workspace host, emails, and role ARNs become placeholders. results/ is
  committed to a public repo.
"""

from __future__ import annotations

import json
import os
import re
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
    # Databricks
    databricks_profile: str
    warehouse_id: str
    uc_foreign_catalog: str
    uc_connection: str
    uc_service_credential: str
    uc_storage_credential: str
    uc_external_location: str
    # AWS
    aws_profile: str
    aws_region: str
    aws_account_id: str
    s3_bucket: str
    glue_database: str
    uc_federation_role_arn: str
    athena_workgroup: str
    # Shape
    table_prefix: str

    @property
    def warehouse_path(self) -> str:
        return f"s3://{self.s3_bucket}/warehouse"

    @property
    def uc_metadata_root(self) -> str:
        return f"s3://{self.s3_bucket}/uc-metadata"

    @property
    def glue_rest_uri(self) -> str:
        return f"https://glue.{self.aws_region}.amazonaws.com/iceberg"


def load_config() -> Config:
    _load_env_file()
    required = [
        "DATABRICKS_WAREHOUSE_ID",
        "AWS_REGION",
        "AWS_ACCOUNT_ID",
        "S3_BUCKET",
        "GLUE_DATABASE",
        "UC_FEDERATION_ROLE_ARN",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise SystemExit(
            f"Missing required config: {missing}. Copy template.env to dev.env and run "
            "`make tf-apply` for the AWS values."
        )
    return Config(
        databricks_profile=os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT"),
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        uc_foreign_catalog=os.environ.get("UC_FOREIGN_CATALOG", "glue_iceberg_list_eval"),
        uc_connection=os.environ.get("UC_CONNECTION", "glue_iceberg_list_eval_conn"),
        uc_service_credential=os.environ.get("UC_SERVICE_CREDENTIAL", "glue_iceberg_list_eval_svc"),
        uc_storage_credential=os.environ.get("UC_STORAGE_CREDENTIAL", "glue_iceberg_list_eval_stg"),
        uc_external_location=os.environ.get("UC_EXTERNAL_LOCATION", "glue_iceberg_list_eval_loc"),
        aws_profile=os.environ.get("AWS_PROFILE", ""),
        aws_region=os.environ["AWS_REGION"],
        aws_account_id=os.environ["AWS_ACCOUNT_ID"],
        s3_bucket=os.environ["S3_BUCKET"],
        glue_database=os.environ["GLUE_DATABASE"],
        uc_federation_role_arn=os.environ["UC_FEDERATION_ROLE_ARN"],
        athena_workgroup=os.environ.get("ATHENA_WORKGROUP", ""),
        table_prefix=os.environ.get("TABLE_PREFIX", "t"),
    )


# --- Databricks ---------------------------------------------------------------


def databricks_config(cfg: Config):
    """Databricks SDK Config for the named profile (OAuth/SSO, never a PAT)."""
    from databricks.sdk.core import Config as SdkConfig

    sdk = SdkConfig(profile=cfg.databricks_profile)
    tok = sdk.oauth_token().access_token
    if tok.startswith("dapi"):
        raise SystemExit("refusing to run with a PAT; use an OAuth/SSO profile")
    return sdk


def workspace_client(cfg: Config):
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(config=databricks_config(cfg))


class DbsqlError(RuntimeError):
    """Server-side statement failure. .message is the verbatim server text."""

    def __init__(self, message: str, error_code: str | None = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code


def dbsql_exec(cfg: Config, statement: str, w=None, catalog: str | None = None) -> list[list]:
    """Run one statement on the serverless warehouse; return rows.

    Failures raise DbsqlError carrying the server message + error_code. Probes
    catch it and record the message — the error class IS the finding.
    """
    w = w or workspace_client(cfg)
    kwargs = {"warehouse_id": cfg.warehouse_id, "statement": statement, "wait_timeout": "50s"}
    if catalog:
        kwargs["catalog"] = catalog
    resp = w.statement_execution.execute_statement(**kwargs)
    while resp.status and resp.status.state and resp.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1.0)
        resp = w.statement_execution.get_statement(resp.statement_id)
    state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    if state != "SUCCEEDED":
        err = resp.status.error if resp.status else None
        message = err.message if err and err.message else state
        code = err.error_code.value if err and err.error_code else None
        raise DbsqlError(message, code)
    return resp.result.data_array if resp.result and resp.result.data_array else []


def dbsql_identity(cfg: Config, w=None) -> str:
    rows = dbsql_exec(cfg, "SELECT current_user()", w)
    return rows[0][0]


def probe_sql(cfg: Config, statement: str, w=None) -> dict:
    """Run a statement and return a uniform outcome dict — never raises.

    {"ok": bool, "rows": [...], "error_class": "...", "error": "..."}. error_class
    is the bracketed Databricks error condition when present, e.g.
    INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE.
    """
    started = time.perf_counter()
    try:
        rows = dbsql_exec(cfg, statement, w)
        return {"ok": True, "rows": rows[:20], "row_count": len(rows),
                "elapsed_ms": round((time.perf_counter() - started) * 1000)}
    except DbsqlError as e:
        return {"ok": False, "error_class": error_class(e.message), "error_code": e.error_code,
                "error": e.message[:1500],
                "elapsed_ms": round((time.perf_counter() - started) * 1000)}


def error_class(message: str) -> str | None:
    m = re.search(r"\[([A-Z0-9_.]+)\]", message or "")
    return m.group(1) if m else None


# --- AWS ----------------------------------------------------------------------


def boto_session(cfg: Config):
    import boto3

    kwargs = {"region_name": cfg.aws_region}
    if cfg.aws_profile:
        kwargs["profile_name"] = cfg.aws_profile
    return boto3.Session(**kwargs)


def aws_identity(cfg: Config) -> str:
    return boto_session(cfg).client("sts").get_caller_identity()["Arn"]


def glue_table(cfg: Config, name: str) -> dict:
    """Raw Glue GetTable response body — the StorageDescriptor is the evidence."""
    return boto_session(cfg).client("glue").get_table(
        DatabaseName=cfg.glue_database, Name=name
    )["Table"]


def glue_sd_types(table: dict) -> dict[str, str]:
    """{column_name: StorageDescriptor.Type} for a Glue table dict."""
    return {c["Name"]: c["Type"] for c in table.get("StorageDescriptor", {}).get("Columns", [])}


def glue_table_summary(table: dict) -> dict:
    p = table.get("Parameters", {})
    return {
        "table_type": p.get("table_type"),
        "metadata_location": bool(p.get("metadata_location")),
        "sd_location": table.get("StorageDescriptor", {}).get("Location"),
        "sd_types": glue_sd_types(table),
    }


# --- Iceberg writers ----------------------------------------------------------
#
# Two catalogs, same Glue database, same S3 warehouse. The ONLY thing that
# differs is which code path writes the Glue StorageDescriptor:
#
#   "rest"  -> AWS Glue Iceberg REST endpoint (server-side SD mapping)
#   "glue"  -> pyiceberg GlueCatalog (client-side, mirrors the Java
#              IcebergToGlueConverter: list -> array<...>)

WRITERS = ("rest", "glue")


def iceberg_catalog(cfg: Config, writer: str):
    from pyiceberg.catalog import load_catalog

    common = {"warehouse": cfg.warehouse_path}
    if cfg.aws_profile:
        common["profile_name"] = cfg.aws_profile
    if writer == "rest":
        return load_catalog(
            "glue_rest",
            **{
                "type": "rest",
                "uri": cfg.glue_rest_uri,
                "warehouse": cfg.aws_account_id,
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "glue",
                "rest.signing-region": cfg.aws_region,
                **({"profile_name": cfg.aws_profile} if cfg.aws_profile else {}),
            },
        )
    if writer == "glue":
        return load_catalog(
            "glue_native",
            **{"type": "glue", "glue.region": cfg.aws_region, **common},
        )
    raise ValueError(f"unknown writer {writer!r}")


# --- Type-shape catalog -------------------------------------------------------
#
# One entry per shape the matrix sweeps. `build` returns a pyiceberg Schema;
# `row` returns one record matching it. Field ids are assigned explicitly so
# the schemas are stable across runs.


def _shapes():
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        ListType,
        LongType,
        MapType,
        NestedField,
        StringType,
        StructType,
    )

    def primitives():
        return Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=False),
        )

    def list_primitive():
        return Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "list_ids", ListType(3, LongType(), element_required=False), required=False),
        )

    def list_struct():
        return Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(
                2,
                "items",
                ListType(
                    3,
                    StructType(
                        NestedField(4, "k", StringType(), required=False),
                        NestedField(5, "v", LongType(), required=False),
                    ),
                    element_required=False,
                ),
                required=False,
            ),
        )

    def map_primitive():
        return Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(
                2, "attrs", MapType(3, StringType(), 4, StringType(), value_required=False), required=False
            ),
        )

    def struct_with_list():
        return Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(
                2,
                "payload",
                StructType(
                    NestedField(3, "tags", ListType(4, StringType(), element_required=False), required=False),
                    NestedField(5, "n", LongType(), required=False),
                ),
                required=False,
            ),
        )

    def nested_struct_required():
        # Documented Databricks Iceberg limitation: "Nested STRUCT with required fields".
        return Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(
                2,
                "outer",
                StructType(
                    NestedField(
                        3,
                        "inner",
                        StructType(NestedField(4, "must", LongType(), required=True)),
                        required=True,
                    ),
                ),
                required=True,
            ),
        )

    return {
        "primitives": (primitives, lambda i: {"id": i, "name": f"n{i}"}),
        "list_primitive": (list_primitive, lambda i: {"id": i, "list_ids": [i, i + 1]}),
        "list_struct": (list_struct, lambda i: {"id": i, "items": [{"k": "a", "v": i}]}),
        "map_primitive": (map_primitive, lambda i: {"id": i, "attrs": {"k": "v"}}),
        "struct_with_list": (struct_with_list, lambda i: {"id": i, "payload": {"tags": ["x"], "n": i}}),
        "nested_struct_required": (
            nested_struct_required,
            lambda i: {"id": i, "outer": {"inner": {"must": i}}},
        ),
    }


# Kept as a literal (not derived from _shapes()) so importing this module never
# imports pyiceberg — unit tests and `make help` stay dependency-free.
SHAPES = (
    "primitives",
    "list_primitive",
    "list_struct",
    "map_primitive",
    "struct_with_list",
    "nested_struct_required",
)


def shape_schema(name: str):
    return _shapes()[name][0]()


def shape_row(name: str, i: int) -> dict:
    return _shapes()[name][1](i)


def table_name(cfg: Config, writer: str, shape: str) -> str:
    return f"{cfg.table_prefix}_{writer}_{shape}"


def ensure_table(cfg: Config, writer: str, shape: str, rows: int = 3, replace: bool = False):
    """Create (or recreate) one (writer, shape) table and append `rows` records.

    Returns the pyiceberg Table. Idempotent: existing table is reused unless
    replace=True. Append is always performed so every call produces a fresh
    commit — that is what row 06 (SD patch durability) relies on.
    """
    import pyarrow as pa
    from pyiceberg.exceptions import NoSuchTableError

    cat = iceberg_catalog(cfg, writer)
    ident = (cfg.glue_database, table_name(cfg, writer, shape))
    if replace:
        try:
            cat.drop_table(ident)
        except NoSuchTableError:
            pass
    try:
        tbl = cat.load_table(ident)
    except NoSuchTableError:
        tbl = cat.create_table(
            ident,
            schema=shape_schema(shape),
            location=f"{cfg.warehouse_path}/{cfg.glue_database}/{ident[1]}",
        )
    schema = tbl.schema().as_arrow()
    data = pa.Table.from_pylist([shape_row(shape, i) for i in range(rows)], schema=schema)
    tbl.append(data)
    return tbl


# --- Redaction ----------------------------------------------------------------


def redact(obj, cfg: Config):
    """Replace real coordinates with placeholders anywhere in a JSON-able object."""
    # Most specific first: the role ARN and bucket share a name prefix, and the
    # account id is a substring of both.
    subs = [
        (re.escape(cfg.uc_federation_role_arn), "arn:aws:iam::<aws-account-id>:role/<role>"),
        (re.escape(cfg.s3_bucket), "<bucket>"),
        (re.escape(cfg.aws_account_id), "<aws-account-id>"),
        (r"arn:aws:(iam|sts)::\d{12}:[^\s\"']+", r"arn:aws:\1::<aws-account-id>:<principal>"),
        (r"https://[a-zA-Z0-9.-]+\.cloud\.databricks\.com", "https://<workspace>.cloud.databricks.com"),
        (r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "<email>"),
        (r"\b\d{12}\b", "<aws-account-id>"),
    ]

    def _s(s: str) -> str:
        for pat, rep in subs:
            s = re.sub(pat, rep, s)
        return s

    if isinstance(obj, str):
        return _s(obj)
    if isinstance(obj, dict):
        return {(_s(k) if isinstance(k, str) else k): redact(v, cfg) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact(v, cfg) for v in obj]
    return obj


# --- Results ------------------------------------------------------------------


def record_result(cfg: Config, row_key: str, payload: dict) -> None:
    """Merge one matrix row into results/matrix_results.json.

    Fail closed: refuses rows that carry no proven identity. Redacts on the way
    out. Set RESULT_KEY_SUFFIX to record a rerun under its own key.
    """
    if not payload.get("identity"):
        raise SystemExit(f"refusing to record {row_key}: no proven identity in payload")
    row_key = f"{row_key}{os.environ.get('RESULT_KEY_SUFFIX', '')}"
    note = os.environ.get("RESULT_RUN_NOTE")
    if note:
        payload = payload | {"run_note": note}
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if RESULTS_PATH.exists() and RESULTS_PATH.read_text().strip():
        existing = json.loads(RESULTS_PATH.read_text())
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    existing[row_key] = redact(payload | {"recorded_at": stamp}, cfg)
    RESULTS_PATH.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n")
    print(f"recorded {row_key} -> {RESULTS_PATH}")


# --- Output -------------------------------------------------------------------


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def ok(msg: str) -> None:
    print(f"  ✅ {msg}")


def fail(msg: str) -> None:
    print(f"  ❌ {msg}")


def note(msg: str) -> None:
    print(f"  · {msg}")
