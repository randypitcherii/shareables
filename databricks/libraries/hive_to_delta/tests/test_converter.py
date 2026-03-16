"""Unit tests for hive_to_delta.converter module.

Tests the refactored converter with:
- _convert_one_table: shared internal pipeline
- convert_table: Tier 1 simple API
- convert: Tier 2 composable bulk API
"""

from unittest.mock import MagicMock, call, patch

import pytest

from hive_to_delta.models import ConversionResult, ParquetFileInfo, TableInfo


# =============================================================================
# _convert_one_table tests
# =============================================================================


class TestConvertOneTable:
    """Tests for the shared internal pipeline."""

    @patch("hive_to_delta.converter.write_delta_log")
    @patch("hive_to_delta.converter.generate_delta_log")
    @patch("hive_to_delta.converter.build_delta_schema_from_glue")
    def test_with_glue_columns(self, mock_schema, mock_gen, mock_write):
        """table_info.columns is not None -> uses build_delta_schema_from_glue."""
        from hive_to_delta.converter import _convert_one_table

        mock_schema.return_value = {"type": "struct", "fields": []}
        mock_gen.return_value = '{"protocol":{}}'
        mock_write.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"

        spark = MagicMock()
        columns = [{"Name": "id", "Type": "bigint"}]
        table_info = TableInfo(
            name="my_table",
            location="s3://bucket/table",
            columns=columns,
            partition_keys=[],
        )
        files = [ParquetFileInfo(path="s3://bucket/table/part-0.parquet", size=100)]

        result = _convert_one_table(spark, table_info, files, "cat", "sch")

        assert result.success is True
        mock_schema.assert_called_once_with(columns)
        assert result.file_count == 1

    @patch("hive_to_delta.converter.write_delta_log")
    @patch("hive_to_delta.converter.generate_delta_log")
    @patch("hive_to_delta.converter.build_delta_schema_from_spark")
    def test_with_spark_inference(self, mock_spark_schema, mock_gen, mock_write):
        """table_info.columns is None -> reads parquet, uses build_delta_schema_from_spark."""
        from hive_to_delta.converter import _convert_one_table

        mock_spark_schema.return_value = {"type": "struct", "fields": []}
        mock_gen.return_value = '{"protocol":{}}'
        mock_write.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"

        spark = MagicMock()
        spark_schema_obj = MagicMock()
        spark.read.parquet.return_value.schema = spark_schema_obj

        table_info = TableInfo(
            name="my_table",
            location="s3://bucket/table",
            columns=None,
            partition_keys=[],
        )
        files = [
            ParquetFileInfo(path="s3://bucket/table/part-0.parquet", size=100),
            ParquetFileInfo(path="s3://bucket/table/part-1.parquet", size=200),
        ]

        result = _convert_one_table(spark, table_info, files, "cat", "sch")

        assert result.success is True
        spark.read.parquet.assert_called_once_with("s3://bucket/table/part-0.parquet")
        mock_spark_schema.assert_called_once_with(spark_schema_obj)
        assert result.file_count == 2

    @patch("hive_to_delta.converter.write_delta_log")
    @patch("hive_to_delta.converter.generate_delta_log")
    @patch("hive_to_delta.converter.build_delta_schema_from_glue")
    def test_creates_schema_and_table(self, mock_schema, mock_gen, mock_write):
        """Verify SQL statements executed: CREATE SCHEMA, DROP TABLE, CREATE TABLE."""
        from hive_to_delta.converter import _convert_one_table

        mock_schema.return_value = {"type": "struct", "fields": []}
        mock_gen.return_value = '{"protocol":{}}'
        mock_write.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"

        spark = MagicMock()
        table_info = TableInfo(
            name="events",
            location="s3://bucket/events",
            columns=[{"Name": "id", "Type": "bigint"}],
            partition_keys=[],
        )
        files = [ParquetFileInfo(path="s3://bucket/events/part-0.parquet", size=100)]

        _convert_one_table(spark, table_info, files, "cat", "sch")

        sql_calls = [c.args[0].strip() for c in spark.sql.call_args_list]

        # Check CREATE SCHEMA
        assert any("CREATE SCHEMA IF NOT EXISTS `cat`.`sch`" in s for s in sql_calls)
        # Check DROP TABLE
        assert any("DROP TABLE IF EXISTS `cat`.`sch`.`events`" in s for s in sql_calls)
        # Check CREATE TABLE with LOCATION
        assert any(
            "CREATE TABLE `cat`.`sch`.`events`" in s and "USING DELTA" in s and "s3://bucket/events" in s
            for s in sql_calls
        )

    def test_no_files_returns_failure(self):
        """Empty files list -> failed ConversionResult."""
        from hive_to_delta.converter import _convert_one_table

        spark = MagicMock()
        table_info = TableInfo(
            name="empty_table",
            location="s3://bucket/empty",
            columns=[{"Name": "id", "Type": "bigint"}],
            partition_keys=[],
        )

        result = _convert_one_table(spark, table_info, [], "cat", "sch")

        assert result.success is False
        assert "No parquet files found" in result.error
        assert result.file_count == 0

    @patch("hive_to_delta.converter.write_delta_log")
    @patch("hive_to_delta.converter.generate_delta_log")
    @patch("hive_to_delta.converter.build_delta_schema_from_glue")
    def test_target_table_name_override(self, mock_schema, mock_gen, mock_write):
        """table_info.target_table_name='raw_events' -> target_table='cat.sch.raw_events'."""
        from hive_to_delta.converter import _convert_one_table

        mock_schema.return_value = {"type": "struct", "fields": []}
        mock_gen.return_value = '{"protocol":{}}'
        mock_write.return_value = "s3://bucket/table/_delta_log/00000000000000000000.json"

        spark = MagicMock()
        table_info = TableInfo(
            name="events",
            location="s3://bucket/events",
            target_table_name="raw_events",
            columns=[{"Name": "id", "Type": "bigint"}],
            partition_keys=[],
        )
        files = [ParquetFileInfo(path="s3://bucket/events/part-0.parquet", size=100)]

        result = _convert_one_table(spark, table_info, files, "cat", "sch")

        assert result.success is True
        assert result.target_table == "cat.sch.raw_events"
        # Verify CREATE TABLE used the override name
        sql_calls = [c.args[0].strip() for c in spark.sql.call_args_list]
        assert any("`cat`.`sch`.`raw_events`" in s for s in sql_calls)


# =============================================================================
# convert_table tests (Tier 1)
# =============================================================================


class TestConvertTable:
    """Tests for the Tier 1 simple API."""

    @patch("hive_to_delta.converter._convert_one_table")
    def test_basic_call(self, mock_convert_one):
        """Validates df, collects rows, calls _convert_one_table."""
        from hive_to_delta.converter import convert_table

        mock_convert_one.return_value = ConversionResult(
            source_table="my_table",
            target_table="cat.sch.my_table",
            success=True,
            file_count=2,
        )

        spark = MagicMock()
        # Build a mock DataFrame with correct columns and dtypes
        files_df = MagicMock()
        files_df.columns = ["file_path", "size"]
        files_df.dtypes = [("file_path", "string"), ("size", "bigint")]
        row1 = MagicMock()
        row1.file_path = "s3://bucket/table/part-0.parquet"
        row1.size = 100
        row2 = MagicMock()
        row2.file_path = "s3://bucket/table/part-1.parquet"
        row2.size = 200
        files_df.collect.return_value = [row1, row2]

        result = convert_table(
            spark, files_df, "s3://bucket/table/", "cat", "sch", "my_table"
        )

        assert result.success is True
        mock_convert_one.assert_called_once()
        call_args = mock_convert_one.call_args
        # Check the files list passed
        passed_files = call_args[1]["files"] if "files" in call_args[1] else call_args[0][2]
        assert len(passed_files) == 2
        assert passed_files[0].path == "s3://bucket/table/part-0.parquet"

    @patch("hive_to_delta.converter._convert_one_table")
    def test_with_partition_columns(self, mock_convert_one):
        """partition_columns=['year'] -> TableInfo has partition_keys."""
        from hive_to_delta.converter import convert_table

        mock_convert_one.return_value = ConversionResult(
            source_table="my_table",
            target_table="cat.sch.my_table",
            success=True,
            file_count=1,
        )

        spark = MagicMock()
        files_df = MagicMock()
        files_df.columns = ["file_path", "size"]
        files_df.dtypes = [("file_path", "string"), ("size", "bigint")]
        row = MagicMock()
        row.file_path = "s3://bucket/table/year=2024/part-0.parquet"
        row.size = 100
        files_df.collect.return_value = [row]

        result = convert_table(
            spark, files_df, "s3://bucket/table", "cat", "sch", "my_table",
            partition_columns=["year"],
        )

        assert result.success is True
        call_args = mock_convert_one.call_args
        passed_table_info = call_args[0][1]
        assert passed_table_info.partition_keys == ["year"]
        # Check partition values were parsed
        passed_files = call_args[0][2]
        assert passed_files[0].partition_values == {"year": "2024"}

    def test_invalid_dataframe_raises(self):
        """Wrong columns -> ValueError."""
        from hive_to_delta.converter import convert_table

        spark = MagicMock()
        files_df = MagicMock()
        files_df.columns = ["wrong_col", "size"]
        files_df.dtypes = [("wrong_col", "string"), ("size", "bigint")]

        with pytest.raises(ValueError, match="file_path"):
            convert_table(spark, files_df, "s3://bucket/table", "cat", "sch", "my_table")


# =============================================================================
# convert tests (Tier 2)
# =============================================================================


class TestConvert:
    """Tests for the Tier 2 composable bulk API."""

    @patch("hive_to_delta.converter._convert_one_table")
    def test_discovery_and_listing(self, mock_convert_one):
        """discovery.discover called, listing.list_files called, convert called."""
        from hive_to_delta.converter import convert

        table = TableInfo(name="t1", location="s3://bucket/t1", partition_keys=[])
        files = [ParquetFileInfo(path="s3://bucket/t1/part-0.parquet", size=100)]

        mock_convert_one.return_value = ConversionResult(
            source_table="t1", target_table="cat.sch.t1", success=True, file_count=1,
        )

        spark = MagicMock()
        discovery = MagicMock()
        discovery.discover.return_value = [table]
        listing = MagicMock()
        listing.list_files.return_value = files

        results = convert(spark, discovery, listing, "cat", "sch")

        discovery.discover.assert_called_once_with(spark)
        listing.list_files.assert_called_once_with(spark, table)
        mock_convert_one.assert_called_once()
        assert len(results) == 1
        assert results[0].success is True

    @patch("hive_to_delta.converter.run_parallel")
    @patch("hive_to_delta.converter._convert_one_table")
    def test_parallel_execution(self, mock_convert_one, mock_run_parallel):
        """Multiple tables -> run_parallel used."""
        from hive_to_delta.converter import convert

        t1 = TableInfo(name="t1", location="s3://bucket/t1", partition_keys=[])
        t2 = TableInfo(name="t2", location="s3://bucket/t2", partition_keys=[])
        f1 = [ParquetFileInfo(path="s3://bucket/t1/part-0.parquet", size=100)]
        f2 = [ParquetFileInfo(path="s3://bucket/t2/part-0.parquet", size=200)]

        r1 = ConversionResult(source_table="t1", target_table="cat.sch.t1", success=True, file_count=1)
        r2 = ConversionResult(source_table="t2", target_table="cat.sch.t2", success=True, file_count=1)
        mock_run_parallel.return_value = [r1, r2]

        spark = MagicMock()
        discovery = MagicMock()
        discovery.discover.return_value = [t1, t2]
        listing = MagicMock()
        listing.list_files.side_effect = [f1, f2]

        results = convert(spark, discovery, listing, "cat", "sch", max_workers=2)

        mock_run_parallel.assert_called_once()
        assert len(results) == 2

    def test_empty_discovery_returns_empty(self):
        """No tables found -> empty list, listing never called."""
        from hive_to_delta.converter import convert

        spark = MagicMock()
        discovery = MagicMock()
        discovery.discover.return_value = []
        listing = MagicMock()

        results = convert(spark, discovery, listing, "cat", "sch")

        assert results == []
        listing.list_files.assert_not_called()
