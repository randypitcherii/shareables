"""Infrastructure integration tests for the composable pipeline APIs.

Tests the NEW composable pipeline (convert_table, convert, GlueDiscovery,
S3Listing, InventoryListing) against real Databricks + AWS infrastructure.

These tests:
- FAIL LOUDLY if infrastructure is unavailable (no silent skips)
- Are marked with pytest.mark.infrastructure for selective running
- Use unique table names (composable_test_ prefix) to avoid conflicts

Requirements:
- Live Databricks workspace (DATABRICKS_CONFIG_PROFILE or Databricks Connect config)
- AWS credentials with access to Glue and S3
- Test Glue database with standard_table (HIVE_TO_DELTA_TEST_GLUE_DATABASE)
"""

from typing import Dict
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config

from hive_to_delta import (
    ConversionResult,
    GlueDiscovery,
    InventoryListing,
    ParquetFileInfo,
    S3Listing,
    TableInfo,
    convert,
    convert_table,
    validate_files_df,
)

# =============================================================================
# Configuration
# =============================================================================

TABLE_PREFIX = "composable_test_"


# =============================================================================
# Helpers
# =============================================================================


def parse_s3_path(s3_path: str) -> tuple:
    """Parse s3://bucket/key into (bucket, key)."""
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip("/")


def get_region_for_bucket(bucket: str) -> str:
    """Determine the AWS region for a bucket based on naming convention."""
    if "west-2" in bucket:
        return "us-west-2"
    return "us-east-1"


def delta_log_exists(s3_clients: Dict[str, boto3.client], table_location: str) -> bool:
    """Check if Delta log exists at the given table location."""
    bucket, prefix = parse_s3_path(table_location.rstrip("/"))
    delta_log_key = f"{prefix}/_delta_log/00000000000000000000.json"
    region = get_region_for_bucket(bucket)
    s3 = s3_clients[region]

    try:
        s3.head_object(Bucket=bucket, Key=delta_log_key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def s3_clients() -> Dict[str, boto3.client]:
    """S3 clients for each region."""
    regions = ["us-east-1", "us-west-2"]
    clients = {}
    for region in regions:
        config = Config(
            region_name=region,
            retries={"max_attempts": 3, "mode": "standard"},
        )
        clients[region] = boto3.client("s3", config=config)
    return clients


@pytest.fixture(scope="module", autouse=True)
def require_infrastructure(spark):
    """Assert that Databricks infrastructure is available. Fails loudly if not."""
    try:
        result = spark.sql("SELECT 1 as test").collect()
        assert result[0][0] == 1, "Spark session is not functional"
    except Exception as e:
        pytest.fail(
            f"Infrastructure not available: {e}\n"
            f"These tests require a live Databricks workspace. "
            f"Set DATABRICKS_CONFIG_PROFILE or configure Databricks Connect."
        )


@pytest.fixture(scope="module")
def glue_table_info(glue_database, aws_region):
    """Discover the standard_table from Glue and return its TableInfo."""
    discovery = GlueDiscovery(database=glue_database, pattern="standard_table", region=aws_region)
    # We call discover with None since GlueDiscovery doesn't use spark
    tables = discovery.discover(None)
    if not tables:
        pytest.fail(
            f"Could not find 'standard_table' in Glue database '{glue_database}'. "
            f"Ensure the test Glue database is set up correctly."
        )
    return tables[0]


@pytest.fixture(scope="module")
def real_file_list(glue_table_info, glue_database, aws_region):
    """Get real S3 file paths for standard_table using S3Listing."""
    listing = S3Listing(region=aws_region, glue_database=glue_database)
    files = listing.list_files(None, glue_table_info)
    if not files:
        pytest.fail(
            f"No parquet files found for standard_table at {glue_table_info.location}. "
            f"Ensure the test data exists in S3."
        )
    return files


# =============================================================================
# Test Category 1: convert_table() (Tier 1) against real infra
# =============================================================================


@pytest.mark.infrastructure
class TestConvertTable:
    """Tests for the Tier 1 convert_table() API with real infrastructure."""

    def test_convert_table_basic(
        self,
        spark,
        s3_clients,
        target_catalog,
        target_schema,
        cleanup_tables,
        glue_table_info,
        real_file_list,
        aws_region,
    ):
        """convert_table() creates a real Delta table from a Spark DataFrame of file paths."""
        table_name = f"{TABLE_PREFIX}basic"
        fq_table = f"{target_catalog}.{target_schema}.{table_name}"
        cleanup_tables.append(fq_table)

        # Build a real Spark DataFrame from the discovered file list
        file_rows = [(f.path, f.size) for f in real_file_list]
        files_df = spark.createDataFrame(file_rows, ["file_path", "size"])

        result = convert_table(
            spark=spark,
            files_df=files_df,
            table_location=glue_table_info.location,
            target_catalog=target_catalog,
            target_schema=target_schema,
            target_table=table_name,
            partition_columns=glue_table_info.partition_keys,
            aws_region=aws_region,
        )

        # Verify ConversionResult
        assert isinstance(result, ConversionResult)
        assert result.success, f"convert_table() failed: {result.error}"
        assert result.file_count > 0, "Expected at least one file"
        assert result.delta_log_location, "Delta log location should not be empty"

        # Verify Delta log exists on S3
        table_location = result.delta_log_location.rsplit("/_delta_log/", 1)[0]
        assert delta_log_exists(s3_clients, table_location), (
            f"Delta log not found on S3 at {table_location}"
        )

        # Verify table is queryable
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {fq_table}").collect()[0]["cnt"]
        assert row_count > 0, f"Table {fq_table} should have rows, got {row_count}"

    def test_convert_table_with_partitions(
        self,
        spark,
        s3_clients,
        target_catalog,
        target_schema,
        cleanup_tables,
        glue_table_info,
        real_file_list,
        aws_region,
    ):
        """convert_table() correctly handles partition columns."""
        table_name = f"{TABLE_PREFIX}partitioned"
        fq_table = f"{target_catalog}.{target_schema}.{table_name}"
        cleanup_tables.append(fq_table)

        # Must have partition keys for this test to be meaningful
        assert glue_table_info.partition_keys, (
            "standard_table should have partition keys for this test"
        )

        file_rows = [(f.path, f.size) for f in real_file_list]
        files_df = spark.createDataFrame(file_rows, ["file_path", "size"])

        result = convert_table(
            spark=spark,
            files_df=files_df,
            table_location=glue_table_info.location,
            target_catalog=target_catalog,
            target_schema=target_schema,
            target_table=table_name,
            partition_columns=glue_table_info.partition_keys,
            aws_region=aws_region,
        )

        assert result.success, f"convert_table() with partitions failed: {result.error}"

        # Verify partition columns are registered
        partitions = spark.sql(f"SHOW PARTITIONS {fq_table}").collect()
        assert len(partitions) > 0, "Expected at least one partition"

        # Verify data is readable
        sample = spark.sql(f"SELECT * FROM {fq_table} LIMIT 5").collect()
        assert len(sample) > 0, "Should be able to read sample data"

    def test_convert_table_no_partitions(
        self,
        spark,
        s3_clients,
        target_catalog,
        target_schema,
        cleanup_tables,
        glue_table_info,
        real_file_list,
        aws_region,
    ):
        """convert_table() works without partition columns specified."""
        table_name = f"{TABLE_PREFIX}no_partitions"
        fq_table = f"{target_catalog}.{target_schema}.{table_name}"
        cleanup_tables.append(fq_table)

        file_rows = [(f.path, f.size) for f in real_file_list]
        files_df = spark.createDataFrame(file_rows, ["file_path", "size"])

        result = convert_table(
            spark=spark,
            files_df=files_df,
            table_location=glue_table_info.location,
            target_catalog=target_catalog,
            target_schema=target_schema,
            target_table=table_name,
            partition_columns=None,
            aws_region=aws_region,
        )

        assert result.success, f"convert_table() without partitions failed: {result.error}"
        assert result.file_count > 0

        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {fq_table}").collect()[0]["cnt"]
        assert row_count > 0, "Table should have data even without partition columns"


# =============================================================================
# Test Category 2: convert() (Tier 2) with GlueDiscovery + S3Listing
# =============================================================================


@pytest.mark.infrastructure
class TestConvertWithGlueAndS3:
    """Tests for the Tier 2 convert() API with GlueDiscovery + S3Listing."""

    def test_convert_glue_s3_single_table(
        self,
        spark,
        s3_clients,
        target_catalog,
        target_schema,
        cleanup_tables,
        glue_database,
        aws_region,
    ):
        """convert() with GlueDiscovery + S3Listing creates a real queryable table."""
        table_name = f"{TABLE_PREFIX}glue_s3"

        # We need to override the target table name via discovery
        discovery = GlueDiscovery(
            database=glue_database, pattern="standard_table", region=aws_region
        )
        listing = S3Listing(region=aws_region, glue_database=glue_database)

        # Discover and set target name override
        tables = discovery.discover(spark)
        assert len(tables) >= 1, "GlueDiscovery should find standard_table"
        tables[0].target_table_name = table_name

        fq_table = f"{target_catalog}.{target_schema}.{table_name}"
        cleanup_tables.append(fq_table)

        # Use convert() with the discovered tables by running discovery again
        # But we need to override the name. Let's do it manually through the pipeline.
        from hive_to_delta.converter import _convert_one_table

        files = listing.list_files(spark, tables[0])
        assert len(files) > 0, "S3Listing should find files for standard_table"

        result = _convert_one_table(
            spark, tables[0], files, target_catalog, target_schema, aws_region
        )

        assert isinstance(result, ConversionResult)
        assert result.success, f"convert pipeline failed: {result.error}"
        assert result.file_count > 0

        # Verify Delta log on S3
        table_location = result.delta_log_location.rsplit("/_delta_log/", 1)[0]
        assert delta_log_exists(s3_clients, table_location)

        # Verify queryable
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {fq_table}").collect()[0]["cnt"]
        assert row_count > 0

    def test_convert_full_pipeline(
        self,
        spark,
        s3_clients,
        target_catalog,
        target_schema,
        cleanup_tables,
        glue_database,
        aws_region,
    ):
        """convert() end-to-end with GlueDiscovery + S3Listing using the public API."""
        # This uses convert() directly, which will create a table with the Glue name.
        # The standard_table may already exist from test_convert.py, so we use convert()
        # which drops and recreates.
        discovery = GlueDiscovery(
            database=glue_database, pattern="standard_table", region=aws_region
        )
        listing = S3Listing(region=aws_region, glue_database=glue_database)

        results = convert(
            spark=spark,
            discovery=discovery,
            listing=listing,
            target_catalog=target_catalog,
            target_schema=target_schema,
            aws_region=aws_region,
            max_workers=1,
            print_summary=False,
        )

        assert len(results) >= 1, "convert() should return at least one result"

        for result in results:
            assert isinstance(result, ConversionResult)
            assert result.success, f"convert() failed for {result.source_table}: {result.error}"
            assert result.file_count > 0

            # Verify queryable
            row_count = spark.sql(
                f"SELECT COUNT(*) as cnt FROM {result.target_table}"
            ).collect()[0]["cnt"]
            assert row_count > 0, f"Table {result.target_table} should have rows"


# =============================================================================
# Test Category 3: convert() with GlueDiscovery + InventoryListing
# =============================================================================


@pytest.mark.infrastructure
class TestConvertWithInventoryListing:
    """Tests for convert() using GlueDiscovery + InventoryListing against real data."""

    def test_inventory_listing_from_real_files(
        self,
        spark,
        s3_clients,
        target_catalog,
        target_schema,
        cleanup_tables,
        glue_table_info,
        real_file_list,
        aws_region,
    ):
        """InventoryListing built from real S3 file paths produces a valid conversion."""
        table_name = f"{TABLE_PREFIX}inventory"
        fq_table = f"{target_catalog}.{target_schema}.{table_name}"
        cleanup_tables.append(fq_table)

        # Build a real Spark DataFrame from the file list discovered by S3Listing
        file_rows = [(f.path, f.size) for f in real_file_list]
        files_df = spark.createDataFrame(file_rows, ["file_path", "size"])

        # Create InventoryListing from the real DataFrame
        inventory = InventoryListing(files_df)

        # Build a TableInfo with target name override
        table_info = TableInfo(
            name=table_name,
            location=glue_table_info.location,
            columns=glue_table_info.columns,
            partition_keys=glue_table_info.partition_keys,
        )

        # Use InventoryListing to list files
        listed_files = inventory.list_files(spark, table_info)
        assert len(listed_files) > 0, "InventoryListing should return files"
        assert len(listed_files) == len(real_file_list), (
            f"InventoryListing should return same number of files: "
            f"expected {len(real_file_list)}, got {len(listed_files)}"
        )

        # Convert using the internal pipeline
        from hive_to_delta.converter import _convert_one_table

        result = _convert_one_table(
            spark, table_info, listed_files, target_catalog, target_schema, aws_region
        )

        assert result.success, f"InventoryListing conversion failed: {result.error}"
        assert result.file_count == len(real_file_list)

        # Verify Delta log on S3
        table_location = result.delta_log_location.rsplit("/_delta_log/", 1)[0]
        assert delta_log_exists(s3_clients, table_location)

        # Verify table is queryable
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {fq_table}").collect()[0]["cnt"]
        assert row_count > 0, f"Table {fq_table} should have rows"

    def test_inventory_listing_filters_by_location(
        self,
        spark,
        glue_table_info,
        real_file_list,
    ):
        """InventoryListing filters files to only those under the table location."""
        # Build a DataFrame with real files PLUS some decoy files
        real_rows = [(f.path, f.size) for f in real_file_list]
        decoy_rows = [
            ("s3://completely-different-bucket/other/data.parquet", 999),
            ("s3://another-bucket/unrelated/file.parquet", 888),
        ]
        all_rows = real_rows + decoy_rows
        files_df = spark.createDataFrame(all_rows, ["file_path", "size"])

        inventory = InventoryListing(files_df)

        table_info = TableInfo(
            name="filter_test",
            location=glue_table_info.location,
            partition_keys=glue_table_info.partition_keys,
        )

        listed_files = inventory.list_files(spark, table_info)

        # Should only include real files, not decoys
        assert len(listed_files) == len(real_file_list), (
            f"InventoryListing should filter to {len(real_file_list)} files, "
            f"got {len(listed_files)}"
        )
        for f in listed_files:
            assert f.path.startswith(glue_table_info.location), (
                f"File {f.path} should be under {glue_table_info.location}"
            )


# =============================================================================
# Test Category 4: InventoryListing with real Spark DataFrame
# =============================================================================


@pytest.mark.infrastructure
class TestInventoryListingValidation:
    """Tests for InventoryListing validation with a real Spark DataFrame."""

    def test_valid_dataframe_accepted(self, spark, real_file_list):
        """InventoryListing accepts a valid Spark DataFrame with correct columns."""
        file_rows = [(f.path, f.size) for f in real_file_list]
        files_df = spark.createDataFrame(file_rows, ["file_path", "size"])

        # Should not raise
        inventory = InventoryListing(files_df)
        assert inventory.files_df is not None

    def test_invalid_columns_rejected(self, spark):
        """InventoryListing rejects a DataFrame missing required columns."""
        bad_df = spark.createDataFrame([("a",), ("b",)], ["wrong_column"])

        with pytest.raises(ValueError, match="file_path"):
            InventoryListing(bad_df)

    def test_invalid_type_rejected(self, spark):
        """InventoryListing rejects a DataFrame with wrong column types."""
        bad_df = spark.createDataFrame(
            [("path1", "not_an_int"), ("path2", "also_not")],
            ["file_path", "size"],
        )

        with pytest.raises(ValueError, match="size"):
            InventoryListing(bad_df)

    def test_list_files_returns_parquet_file_info(
        self, spark, glue_table_info, real_file_list
    ):
        """InventoryListing.list_files() returns correct ParquetFileInfo objects."""
        file_rows = [(f.path, f.size) for f in real_file_list]
        files_df = spark.createDataFrame(file_rows, ["file_path", "size"])

        inventory = InventoryListing(files_df)

        table_info = TableInfo(
            name="pfi_test",
            location=glue_table_info.location,
            partition_keys=glue_table_info.partition_keys,
        )

        listed_files = inventory.list_files(spark, table_info)

        assert len(listed_files) > 0, "Should return files"

        for f in listed_files:
            assert isinstance(f, ParquetFileInfo), f"Expected ParquetFileInfo, got {type(f)}"
            assert f.path.startswith("s3://"), f"Path should be absolute S3 path: {f.path}"
            assert f.size > 0, f"Size should be positive: {f.size}"
            assert f.is_absolute_path, "is_absolute_path should be True"

            # If there are partition keys, verify partition values were parsed
            if glue_table_info.partition_keys:
                assert len(f.partition_values) == len(glue_table_info.partition_keys), (
                    f"Expected {len(glue_table_info.partition_keys)} partition values, "
                    f"got {len(f.partition_values)}"
                )

    def test_validate_files_df_with_real_dataframe(self, spark, real_file_list):
        """validate_files_df() accepts a real Spark DataFrame."""
        file_rows = [(f.path, f.size) for f in real_file_list]
        files_df = spark.createDataFrame(file_rows, ["file_path", "size"])

        # Should not raise
        validate_files_df(files_df)


# =============================================================================
# Test Category 5: GlueDiscovery integration
# =============================================================================


@pytest.mark.infrastructure
class TestGlueDiscoveryIntegration:
    """Tests for GlueDiscovery against real Glue infrastructure."""

    def test_discover_finds_standard_table(self, spark, glue_database, aws_region):
        """GlueDiscovery finds the standard_table in the test Glue database."""
        discovery = GlueDiscovery(
            database=glue_database, pattern="standard_table", region=aws_region
        )
        tables = discovery.discover(spark)

        assert len(tables) == 1, f"Expected 1 table, found {len(tables)}"
        table = tables[0]
        assert isinstance(table, TableInfo)
        assert table.name == "standard_table"
        assert table.location.startswith("s3://"), f"Location should be S3 path: {table.location}"
        assert table.columns is not None, "Glue should provide column metadata"
        assert len(table.columns) > 0, "Table should have columns"

    def test_discover_with_wildcard_pattern(self, spark, glue_database, aws_region):
        """GlueDiscovery supports wildcard patterns."""
        discovery = GlueDiscovery(
            database=glue_database, pattern="*_table", region=aws_region
        )
        tables = discovery.discover(spark)

        assert len(tables) >= 1, "Wildcard pattern '*_table' should match at least one table"
        for table in tables:
            assert table.name.endswith("_table"), f"Table name should match pattern: {table.name}"

    def test_discover_returns_partition_keys(self, spark, glue_database, aws_region):
        """GlueDiscovery returns partition key information."""
        discovery = GlueDiscovery(
            database=glue_database, pattern="standard_table", region=aws_region
        )
        tables = discovery.discover(spark)

        assert len(tables) == 1
        table = tables[0]
        # standard_table should be partitioned
        assert isinstance(table.partition_keys, list)
        assert len(table.partition_keys) > 0, "standard_table should have partition keys"


# =============================================================================
# Test Category 6: S3Listing integration
# =============================================================================


@pytest.mark.infrastructure
class TestS3ListingIntegration:
    """Tests for S3Listing against real S3 infrastructure."""

    def test_list_files_finds_parquet(
        self, spark, glue_database, aws_region, glue_table_info
    ):
        """S3Listing discovers real parquet files from S3."""
        listing = S3Listing(region=aws_region, glue_database=glue_database)
        files = listing.list_files(spark, glue_table_info)

        assert len(files) > 0, "S3Listing should find parquet files"
        for f in files:
            assert isinstance(f, ParquetFileInfo)
            assert f.path.endswith(".parquet"), f"Expected parquet file: {f.path}"
            assert f.size > 0, f"File size should be positive: {f.size}"

    def test_list_files_partition_values(
        self, spark, glue_database, aws_region, glue_table_info
    ):
        """S3Listing correctly parses partition values from file paths."""
        listing = S3Listing(region=aws_region, glue_database=glue_database)
        files = listing.list_files(spark, glue_table_info)

        assert len(files) > 0

        if glue_table_info.partition_keys:
            for f in files:
                assert len(f.partition_values) == len(glue_table_info.partition_keys), (
                    f"Expected {len(glue_table_info.partition_keys)} partition values, "
                    f"got {len(f.partition_values)} for {f.path}"
                )
                for key in glue_table_info.partition_keys:
                    assert key in f.partition_values, (
                        f"Missing partition key '{key}' in {f.partition_values}"
                    )
