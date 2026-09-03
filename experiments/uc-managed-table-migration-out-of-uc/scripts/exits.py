"""Catalog-free Python clients used as the exit destinations.

Every function here works against a plain object-storage path (or local
directory in tests). None of them talk to Unity Catalog.
"""

from __future__ import annotations

import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from pyiceberg.catalog.sql import SqlCatalog

ICEBERG_METADATA_DIRS = ("_iceberg/metadata", "metadata")


@dataclass
class ClientOutcome:
    client: str
    succeeded: bool
    rows: int | None = None
    error_type: str | None = None
    error: str | None = None
    detail: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _fail(client: str, exc: Exception) -> ClientOutcome:
    return ClientOutcome(client, False, error_type=type(exc).__name__, error=str(exc)[:600])


def latest_iceberg_metadata(keys: list[str]) -> str | None:
    """Pick the newest Iceberg root metadata file from a listing of relative keys.

    Managed Iceberg writes under ``_iceberg/metadata``; UniForm writes under
    ``metadata``. Files are named ``<version>-<uuid>[.gz].metadata.json`` so a
    lexical sort on the basename orders by version.
    """
    candidates = [
        key
        for key in keys
        if key.endswith("metadata.json")
        and any(key.startswith(f"{d}/") for d in ICEBERG_METADATA_DIRS)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda key: Path(key).name)


def list_s3_keys(location: str, *, region: str) -> list[str]:
    import boto3

    parsed = urlparse(location)
    prefix = parsed.path.strip("/") + "/"
    paginator = boto3.client("s3", region_name=region).get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=parsed.netloc, Prefix=prefix):
        keys.extend(item["Key"][len(prefix) :] for item in page.get("Contents", []))
    return keys


def read_delta_with_deltalake(location: str) -> ClientOutcome:
    try:
        table = DeltaTable(location)
        protocol = table.protocol()
        return ClientOutcome(
            "deltalake",
            True,
            rows=table.to_pyarrow_table().num_rows,
            detail={
                "version": table.version(),
                "min_reader_version": protocol.min_reader_version,
                "min_writer_version": protocol.min_writer_version,
                "reader_features": sorted(protocol.reader_features or []),
            },
        )
    except Exception as exc:  # noqa: BLE001 -- client refusals are the evidence
        return _fail("deltalake", exc)


def read_delta_with_duckdb(location: str, *, region: str | None = None) -> ClientOutcome:
    try:
        con = duckdb.connect()
        con.execute("INSTALL delta; LOAD delta;")
        if location.startswith("s3://"):
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(
                f"CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION '{region or 'us-east-1'}')"
            )
        rows = con.execute(f"SELECT count(*) FROM delta_scan('{location}')").fetchone()[0]
        return ClientOutcome("duckdb-delta", True, rows=int(rows))
    except Exception as exc:  # noqa: BLE001
        return _fail("duckdb-delta", exc)


def register_iceberg_in_new_catalog(
    metadata_location: str, *, append_row: dict[str, Any] | None = None
) -> ClientOutcome:
    """Register existing Iceberg metadata in a brand-new, non-UC catalog and read it.

    Uses pyiceberg's SQLite-backed catalog so the destination has zero shared
    state with Unity Catalog. If ``append_row`` is given, also commits a new
    snapshot through the new catalog to prove it owns the table going forward.
    """
    workdir = tempfile.mkdtemp(prefix="iceberg-exit-")
    try:
        catalog = SqlCatalog(
            "exit", uri=f"sqlite:///{workdir}/catalog.db", warehouse=f"file://{workdir}"
        )
        catalog.create_namespace("exit")
        table = catalog.register_table("exit.migrated", metadata_location)
        rows_at_registration = table.scan().to_arrow().num_rows
        detail: dict[str, Any] = {
            "snapshot_at_registration": table.metadata.current_snapshot_id,
            "rows_at_registration": rows_at_registration,
        }
        rows = rows_at_registration
        if append_row is not None:
            schema = table.schema().as_arrow()
            table.append(pa.Table.from_pylist([append_row], schema=schema))
            table = catalog.load_table("exit.migrated")
            rows = table.scan().to_arrow().num_rows
            detail["snapshot_after_append"] = table.metadata.current_snapshot_id
            detail["new_metadata_written_under_source_path"] = table.metadata_location.startswith(
                metadata_location.rsplit("/", 1)[0]
            )
        return ClientOutcome("pyiceberg-sql-catalog", True, rows=rows, detail=detail)
    except Exception as exc:  # noqa: BLE001
        return _fail("pyiceberg-sql-catalog", exc)


def write_local_delta(path: Path, rows: list[dict[str, Any]]) -> None:
    """Test helper: create a minimal Delta table with the deltalake writer."""
    write_deltalake(str(path), pa.Table.from_pylist(rows))
