"""Tests for hive_to_delta.listing module."""

from unittest.mock import MagicMock, patch

import pytest

from hive_to_delta.listing import (
    InventoryListing,
    Listing,
    S3Listing,
    _parse_partition_values,
    validate_files_df,
)
from hive_to_delta.models import ParquetFileInfo, TableInfo


class TestS3Listing:
    """Tests for S3Listing."""

    @patch("hive_to_delta.listing.scan_partition_files")
    @patch("hive_to_delta.listing.get_glue_partitions")
    def test_partitioned_table(self, mock_get_glue, mock_scan):
        """Partitions returned from Glue, scan_partition_files called with correct locations."""
        mock_get_glue.return_value = [
            {
                "Values": ["2024", "01"],
                "StorageDescriptor": {"Location": "s3://bucket/table/year=2024/month=01/"},
            },
            {
                "Values": ["2024", "02"],
                "StorageDescriptor": {"Location": "s3://bucket/table/year=2024/month=02/"},
            },
        ]
        mock_scan.return_value = [
            ParquetFileInfo(
                path="s3://bucket/table/year=2024/month=01/data.parquet",
                size=100,
                partition_values={"year": "2024", "month": "01"},
            ),
            ParquetFileInfo(
                path="s3://bucket/table/year=2024/month=02/data.parquet",
                size=200,
                partition_values={"year": "2024", "month": "02"},
            ),
        ]

        table = TableInfo(
            name="my_table",
            location="s3://bucket/table",
            partition_keys=["year", "month"],
        )
        listing = S3Listing(region="us-east-1", glue_database="my_db")
        spark = MagicMock()

        result = listing.list_files(spark, table)

        mock_get_glue.assert_called_once_with("my_db", "my_table", region="us-east-1")
        mock_scan.assert_called_once()
        locations_arg = mock_scan.call_args[0][0]
        assert len(locations_arg) == 2
        assert locations_arg[0] == (
            "s3://bucket/table/year=2024/month=01/",
            {"year": "2024", "month": "01"},
        )
        assert locations_arg[1] == (
            "s3://bucket/table/year=2024/month=02/",
            {"year": "2024", "month": "02"},
        )
        assert len(result) == 2

    @patch("hive_to_delta.listing.scan_partition_files")
    @patch("hive_to_delta.listing.get_glue_partitions")
    def test_non_partitioned_table(self, mock_get_glue, mock_scan):
        """No partitions, scans table root."""
        mock_scan.return_value = [
            ParquetFileInfo(
                path="s3://bucket/table/data.parquet",
                size=500,
                partition_values={},
            ),
        ]

        table = TableInfo(
            name="flat_table",
            location="s3://bucket/table/",
            partition_keys=[],
        )
        listing = S3Listing(region="us-east-1")
        spark = MagicMock()

        result = listing.list_files(spark, table)

        mock_get_glue.assert_not_called()
        mock_scan.assert_called_once()
        locations_arg = mock_scan.call_args[0][0]
        assert len(locations_arg) == 1
        assert locations_arg[0] == ("s3://bucket/table/", {})
        assert len(result) == 1

    def test_implements_listing_protocol(self):
        """S3Listing should satisfy the Listing protocol."""
        assert isinstance(S3Listing(), Listing)


class TestInventoryListing:
    """Tests for InventoryListing."""

    def _make_mock_df(self, rows, columns=None, dtypes=None):
        df = MagicMock()
        df.columns = columns or ["file_path", "size"]
        df.dtypes = dtypes or [("file_path", "string"), ("size", "bigint")]
        mock_rows = []
        for file_path, size in rows:
            row = MagicMock()
            row.file_path = file_path
            row.size = size
            mock_rows.append(row)
        df.collect.return_value = mock_rows
        return df

    def test_basic_listing(self):
        """2 files, no partitions -> 2 ParquetFileInfo with empty partition_values."""
        df = self._make_mock_df([
            ("s3://bucket/table/file1.parquet", 100),
            ("s3://bucket/table/file2.parquet", 200),
        ])
        listing = InventoryListing(df)
        table = TableInfo(name="t", location="s3://bucket/table", partition_keys=[])
        spark = MagicMock()

        result = listing.list_files(spark, table)

        assert len(result) == 2
        assert result[0].path == "s3://bucket/table/file1.parquet"
        assert result[0].size == 100
        assert result[0].partition_values == {}
        assert result[1].path == "s3://bucket/table/file2.parquet"
        assert result[1].size == 200
        assert result[1].partition_values == {}

    def test_filters_by_table_location(self):
        """Files not under table.location are excluded."""
        df = self._make_mock_df([
            ("s3://bucket/table_a/file1.parquet", 100),
            ("s3://bucket/table_b/file2.parquet", 200),
            ("s3://bucket/table_a/file3.parquet", 300),
        ])
        listing = InventoryListing(df)
        table = TableInfo(name="a", location="s3://bucket/table_a", partition_keys=[])
        spark = MagicMock()

        result = listing.list_files(spark, table)

        assert len(result) == 2
        assert result[0].path == "s3://bucket/table_a/file1.parquet"
        assert result[1].path == "s3://bucket/table_a/file3.parquet"

    def test_with_partition_columns(self):
        """Files with partition paths, partition values parsed correctly."""
        df = self._make_mock_df([
            ("s3://bucket/table/year=2024/month=01/data.parquet", 100),
            ("s3://bucket/table/year=2024/month=02/data.parquet", 200),
        ])
        listing = InventoryListing(df)
        table = TableInfo(
            name="t",
            location="s3://bucket/table",
            partition_keys=["year", "month"],
        )
        spark = MagicMock()

        result = listing.list_files(spark, table)

        assert len(result) == 2
        assert result[0].partition_values == {"year": "2024", "month": "01"}
        assert result[1].partition_values == {"year": "2024", "month": "02"}

    def test_missing_file_path_column_raises(self):
        """DataFrame without file_path column -> ValueError."""
        df = self._make_mock_df([], columns=["path", "size"])
        with pytest.raises(ValueError, match="file_path"):
            InventoryListing(df)

    def test_missing_size_column_raises(self):
        """DataFrame without size column -> ValueError."""
        df = self._make_mock_df([], columns=["file_path", "bytes"])
        with pytest.raises(ValueError, match="size"):
            InventoryListing(df)

    def test_wrong_file_path_type_raises(self):
        """file_path is int instead of string -> ValueError."""
        df = self._make_mock_df(
            [],
            dtypes=[("file_path", "int"), ("size", "bigint")],
        )
        with pytest.raises(ValueError, match="file_path"):
            InventoryListing(df)

    def test_wrong_size_type_raises(self):
        """size is string instead of int/long/bigint -> ValueError."""
        df = self._make_mock_df(
            [],
            dtypes=[("file_path", "string"), ("size", "string")],
        )
        with pytest.raises(ValueError, match="size"):
            InventoryListing(df)

    def test_partition_value_not_in_path_skipped(self):
        """partition_keys has 'region' but path doesn't have region=X -> empty string."""
        df = self._make_mock_df([
            ("s3://bucket/table/year=2024/data.parquet", 100),
        ])
        listing = InventoryListing(df)
        table = TableInfo(
            name="t",
            location="s3://bucket/table",
            partition_keys=["year", "region"],
        )
        spark = MagicMock()

        result = listing.list_files(spark, table)

        assert len(result) == 1
        assert result[0].partition_values == {"year": "2024", "region": ""}

    def test_implements_listing_protocol(self):
        """InventoryListing should satisfy the Listing protocol."""
        df = self._make_mock_df([])
        assert isinstance(InventoryListing(df), Listing)


class TestValidateFilesDf:
    """Tests for the standalone validate_files_df function."""

    def _make_mock_df(self, columns=None, dtypes=None):
        df = MagicMock()
        df.columns = columns or ["file_path", "size"]
        df.dtypes = dtypes or [("file_path", "string"), ("size", "bigint")]
        return df

    def test_valid_df_passes(self):
        """Valid DataFrame passes without raising."""
        df = self._make_mock_df()
        validate_files_df(df)  # Should not raise

    def test_missing_file_path_raises(self):
        df = self._make_mock_df(columns=["path", "size"])
        with pytest.raises(ValueError, match="file_path"):
            validate_files_df(df)

    def test_missing_size_raises(self):
        df = self._make_mock_df(columns=["file_path", "bytes"])
        with pytest.raises(ValueError, match="size"):
            validate_files_df(df)


class TestParsePartitionValues:
    """Tests for _parse_partition_values module-level function."""

    def test_basic_parsing(self):
        result = _parse_partition_values(
            "s3://bucket/table/year=2024/month=01/data.parquet",
            ["year", "month"],
        )
        assert result == {"year": "2024", "month": "01"}

    def test_missing_key_returns_empty_string(self):
        result = _parse_partition_values(
            "s3://bucket/table/year=2024/data.parquet",
            ["year", "region"],
        )
        assert result == {"year": "2024", "region": ""}

    def test_no_partition_keys(self):
        result = _parse_partition_values(
            "s3://bucket/table/data.parquet",
            [],
        )
        assert result == {}
