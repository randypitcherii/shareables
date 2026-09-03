"""Local tests for the catalog-free Python exit clients (no cloud access)."""

import pyarrow as pa
import pytest
from exit_02_uc_external_reregister import tblproperties_from_error
from exits import (
    latest_iceberg_metadata,
    read_delta_with_deltalake,
    read_delta_with_duckdb,
    register_iceberg_in_new_catalog,
    write_local_delta,
)
from pyiceberg.catalog.sql import SqlCatalog

ROWS = [{"id": 1, "value": "alpha"}, {"id": 2, "value": "beta"}, {"id": 3, "value": "gamma"}]


def test_latest_metadata_prefers_highest_version_in_either_layout():
    keys = [
        "_delta_log/00000000000000000000.json",
        "_iceberg/metadata/00000-aaa.gz.metadata.json",
        "_iceberg/metadata/00001-bbb.gz.metadata.json",
        "_iceberg/metadata/snap-1-x.avro",
    ]
    assert latest_iceberg_metadata(keys) == "_iceberg/metadata/00001-bbb.gz.metadata.json"
    uniform = [
        "metadata/00000-a.metadata.json",
        "metadata/00002-c.metadata.json",
        "yN/part.parquet",
    ]
    assert latest_iceberg_metadata(uniform) == "metadata/00002-c.metadata.json"
    assert latest_iceberg_metadata(["_delta_log/00000000000000000000.json"]) is None


def test_tblproperties_parsed_from_uc_property_mismatch_error():
    error = (
        "[DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY] ...\n== Specified ==\n"
        "delta.parquet.compression.codec=zstd\n\n== Existing ==\n"
        "delta.columnMapping.mode=name\ndelta.enableIcebergCompatV2=true\n"
        "write.metadata.path=s3://b/p/_iceberg/metadata\n"
    )
    assert tblproperties_from_error(error) == (
        " TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.enableIcebergCompatV2'='true', "
        "'write.metadata.path'='s3://b/p/_iceberg/metadata')"
    )
    assert tblproperties_from_error(None) == ""
    assert tblproperties_from_error("[LOCATION_OVERLAP] nope") == ""


def test_zero_catalog_delta_clients_read_a_plain_delta_directory(tmp_path):
    path = tmp_path / "delta"
    write_local_delta(path, ROWS)
    deltalake = read_delta_with_deltalake(str(path))
    duck = read_delta_with_duckdb(str(path))
    assert (deltalake.succeeded, deltalake.rows) == (True, 3)
    assert (duck.succeeded, duck.rows) == (True, 3)


def test_deltalake_reports_protocol_refusal_as_failure(tmp_path):
    outcome = read_delta_with_deltalake(str(tmp_path / "missing"))
    assert not outcome.succeeded
    assert outcome.error_type


@pytest.fixture
def local_iceberg_metadata(tmp_path):
    """Write an Iceberg table with one catalog, return its metadata.json path."""
    warehouse = tmp_path / "source"
    warehouse.mkdir()
    catalog = SqlCatalog("src", uri=f"sqlite:///{tmp_path}/src.db", warehouse=f"file://{warehouse}")
    catalog.create_namespace("ns")
    table = catalog.create_table(
        "ns.t", schema=pa.schema([("id", pa.int32()), ("value", pa.string())])
    )
    table.append(pa.Table.from_pylist(ROWS, schema=table.schema().as_arrow()))
    return catalog.load_table("ns.t").metadata_location


def test_new_catalog_adopts_existing_iceberg_metadata_and_appends(local_iceberg_metadata):
    outcome = register_iceberg_in_new_catalog(
        local_iceberg_metadata, append_row={"id": 4, "value": "delta"}
    )
    assert outcome.succeeded, outcome.error
    assert outcome.detail["rows_at_registration"] == 3
    assert outcome.rows == 4
    assert outcome.detail["new_metadata_written_under_source_path"] is True
