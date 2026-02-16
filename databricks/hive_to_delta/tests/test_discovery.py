"""Unit tests for discovery module."""

from __future__ import annotations

from unittest.mock import patch

from hive_to_delta.discovery import GlueDiscovery
from hive_to_delta.models import TableInfo


def _make_glue_metadata(
    location: str,
    data_columns: list[dict[str, str]],
    partition_keys: list[dict[str, str]] | None = None,
) -> dict:
    """Helper to build a Glue-style table metadata dict."""
    return {
        "StorageDescriptor": {
            "Location": location,
            "Columns": data_columns,
        },
        "PartitionKeys": partition_keys or [],
    }


@patch("hive_to_delta.discovery.list_glue_tables")
@patch("hive_to_delta.discovery.get_glue_table_metadata")
class TestGlueDiscovery:
    """Tests for GlueDiscovery.discover()."""

    def test_discover_single_table(self, mock_get_meta, mock_list):
        mock_list.return_value = ["events"]
        mock_get_meta.return_value = _make_glue_metadata(
            location="s3://bucket/events/",
            data_columns=[
                {"Name": "id", "Type": "bigint"},
                {"Name": "ts", "Type": "timestamp"},
            ],
            partition_keys=[{"Name": "year", "Type": "string"}],
        )

        discovery = GlueDiscovery(database="my_db")
        result = discovery.discover(spark=None)

        assert len(result) == 1
        t = result[0]
        assert isinstance(t, TableInfo)
        assert t.name == "events"
        assert t.location == "s3://bucket/events"  # trailing slash stripped
        assert t.partition_keys == ["year"]
        # columns = data cols + partition key cols
        assert len(t.columns) == 3
        assert {"Name": "year", "Type": "string"} in t.columns

    def test_discover_all_tables(self, mock_get_meta, mock_list):
        mock_list.return_value = ["t1", "t2"]
        mock_get_meta.side_effect = [
            _make_glue_metadata(
                location="s3://bucket/t1",
                data_columns=[{"Name": "id", "Type": "int"}],
            ),
            _make_glue_metadata(
                location="s3://bucket/t2",
                data_columns=[{"Name": "id", "Type": "int"}],
            ),
        ]

        discovery = GlueDiscovery(database="db")
        result = discovery.discover(spark=None)

        assert len(result) == 2
        assert result[0].name == "t1"
        assert result[1].name == "t2"

    def test_discover_with_pattern(self, mock_get_meta, mock_list):
        mock_list.return_value = ["dim_customer", "dim_product"]
        mock_get_meta.side_effect = [
            _make_glue_metadata(
                location="s3://bucket/dim_customer",
                data_columns=[{"Name": "id", "Type": "int"}],
            ),
            _make_glue_metadata(
                location="s3://bucket/dim_product",
                data_columns=[{"Name": "id", "Type": "int"}],
            ),
        ]

        discovery = GlueDiscovery(database="db", pattern="dim_*")
        discovery.discover(spark=None)

        mock_list.assert_called_once_with("db", pattern="dim_*", region="us-east-1")

    def test_discover_no_matching_tables(self, mock_get_meta, mock_list):
        mock_list.return_value = []

        discovery = GlueDiscovery(database="db", pattern="nonexistent_*")
        result = discovery.discover(spark=None)

        assert result == []
        mock_get_meta.assert_not_called()

    def test_non_partitioned_table(self, mock_get_meta, mock_list):
        mock_list.return_value = ["flat_table"]
        mock_get_meta.return_value = _make_glue_metadata(
            location="s3://bucket/flat_table",
            data_columns=[
                {"Name": "id", "Type": "bigint"},
                {"Name": "value", "Type": "string"},
            ],
            partition_keys=[],
        )

        discovery = GlueDiscovery(database="db")
        result = discovery.discover(spark=None)

        t = result[0]
        assert t.partition_keys == []
        assert len(t.columns) == 2
