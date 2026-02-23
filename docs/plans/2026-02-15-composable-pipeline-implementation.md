# Composable Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor hive_to_delta into a composable pipeline with pluggable discovery and listing strategies, adding a simple `convert_table()` entry point and a composable `convert()` bulk API.

**Architecture:** Decompose the pipeline into Discovery (where table metadata comes from) and Listing (how files are enumerated) strategies, with a shared internal `_convert_one_table()` pipeline. Two-tier API: `convert_table()` for simple manual use, `convert()` for automated bulk conversion.

**Tech Stack:** Python 3.10+, PySpark, boto3, pytest, unittest.mock

**Base directory:** `databricks/hive_to_delta/`

---

### Task 1: Add TableInfo model

**Files:**
- Modify: `hive_to_delta/models.py`
- Test: `tests/test_models.py` (create)

**Step 1: Write the failing test**

Create `tests/test_models.py`:

```python
"""Unit tests for data models."""

import pytest
from hive_to_delta.models import TableInfo


class TestTableInfo:
    """Tests for TableInfo dataclass."""

    def test_minimal_construction(self):
        """Test TableInfo with only required fields."""
        info = TableInfo(name="events", location="s3://bucket/events")
        assert info.name == "events"
        assert info.location == "s3://bucket/events"
        assert info.target_table_name is None
        assert info.columns is None
        assert info.partition_keys == []

    def test_full_construction(self):
        """Test TableInfo with all fields."""
        columns = [{"Name": "id", "Type": "bigint"}]
        info = TableInfo(
            name="events",
            location="s3://bucket/events",
            target_table_name="raw_events",
            columns=columns,
            partition_keys=["year", "month"],
        )
        assert info.target_table_name == "raw_events"
        assert info.columns == columns
        assert info.partition_keys == ["year", "month"]

    def test_partition_keys_default_empty(self):
        """Test that partition_keys defaults to empty list."""
        info = TableInfo(name="t", location="s3://b/t")
        assert info.partition_keys == []
        # Verify independent default (not shared mutable)
        info2 = TableInfo(name="t2", location="s3://b/t2")
        info.partition_keys.append("year")
        assert info2.partition_keys == []
```

**Step 2: Run test to verify it fails**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_models.py -v`
Expected: FAIL — `ImportError: cannot import name 'TableInfo' from 'hive_to_delta.models'`

**Step 3: Write minimal implementation**

Add to `hive_to_delta/models.py` after the `VacuumResult` class:

```python
@dataclass
class TableInfo:
    """Metadata about a table to be converted.

    Produced by Discovery strategies, consumed by the conversion pipeline.
    """

    name: str  # Table name
    location: str  # S3 root path for the table data
    target_table_name: Optional[str] = None  # Override for UC table name
    columns: Optional[list[dict[str, str]]] = None  # Glue-style column defs (if available)
    partition_keys: list[str] = field(default_factory=list)
```

**Step 4: Run test to verify it passes**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_models.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add hive_to_delta/models.py tests/test_models.py
git commit -m "Add TableInfo model for composable pipeline"
```

---

### Task 2: Create schema.py — extract and extend schema inference

**Files:**
- Create: `hive_to_delta/schema.py`
- Modify: `hive_to_delta/delta_log.py` (remove schema functions, import from schema.py)
- Test: `tests/test_schema.py` (create)

**Step 1: Write the failing test for build_delta_schema_from_spark**

Create `tests/test_schema.py`:

```python
"""Unit tests for schema inference."""

import pytest
from unittest.mock import MagicMock

from hive_to_delta.schema import (
    build_delta_schema_from_glue,
    build_delta_schema_from_spark,
)


class TestBuildDeltaSchemaFromGlue:
    """Tests for Glue-based schema inference (moved from delta_log)."""

    def test_standard_columns(self):
        """Test standard column types produce correct Delta schema."""
        columns = [
            {"Name": "id", "Type": "bigint"},
            {"Name": "name", "Type": "string"},
        ]
        schema = build_delta_schema_from_glue(columns)
        assert schema["type"] == "struct"
        assert len(schema["fields"]) == 2
        assert schema["fields"][0]["name"] == "id"
        assert schema["fields"][0]["type"] == "long"

    def test_empty_columns_raises(self):
        """Test empty columns raises ValueError."""
        with pytest.raises(ValueError):
            build_delta_schema_from_glue([])


class TestBuildDeltaSchemaFromSpark:
    """Tests for Spark-based schema inference."""

    def _make_field(self, name, type_string):
        """Create a mock StructField."""
        field = MagicMock()
        field.name = name
        field.dataType.simpleString.return_value = type_string
        field.nullable = True
        return field

    def _make_schema(self, fields):
        """Create a mock StructType."""
        schema = MagicMock()
        schema.fields = fields
        return schema

    def test_simple_types(self):
        """Test simple Spark types produce correct Delta schema."""
        spark_schema = self._make_schema([
            self._make_field("id", "bigint"),
            self._make_field("name", "string"),
            self._make_field("amount", "double"),
        ])
        result = build_delta_schema_from_spark(spark_schema)

        assert result["type"] == "struct"
        assert len(result["fields"]) == 3
        assert result["fields"][0] == {
            "name": "id", "type": "bigint", "nullable": True, "metadata": {},
        }
        assert result["fields"][1] == {
            "name": "name", "type": "string", "nullable": True, "metadata": {},
        }

    def test_decimal_type(self):
        """Test decimal(p,s) is preserved."""
        spark_schema = self._make_schema([
            self._make_field("price", "decimal(10,2)"),
        ])
        result = build_delta_schema_from_spark(spark_schema)
        assert result["fields"][0]["type"] == "decimal(10,2)"

    def test_complex_types(self):
        """Test complex Spark types (array, map, struct)."""
        spark_schema = self._make_schema([
            self._make_field("tags", "array<string>"),
            self._make_field("attrs", "map<string,int>"),
        ])
        result = build_delta_schema_from_spark(spark_schema)
        assert result["fields"][0]["type"] == "array<string>"
        assert result["fields"][1]["type"] == "map<string,int>"

    def test_empty_schema_raises(self):
        """Test empty schema raises ValueError."""
        spark_schema = self._make_schema([])
        with pytest.raises(ValueError, match="no fields"):
            build_delta_schema_from_spark(spark_schema)

    def test_nullable_from_field(self):
        """Test nullable is taken from the StructField."""
        field = self._make_field("id", "long")
        field.nullable = False
        spark_schema = self._make_schema([field])
        result = build_delta_schema_from_spark(spark_schema)
        assert result["fields"][0]["nullable"] is False
```

**Step 2: Run test to verify it fails**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_schema.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'hive_to_delta.schema'`

**Step 3: Write schema.py implementation**

Create `hive_to_delta/schema.py`:

```python
"""Schema inference for Delta table creation.

Two paths:
- build_delta_schema_from_glue(): Maps Glue/Hive types to Delta types
- build_delta_schema_from_spark(): Uses Spark StructType directly (for non-Glue sources)
"""

from typing import Any


# Mapping of AWS Glue/Hive types to Delta Lake types
GLUE_TO_DELTA_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "char": "string",
    "varchar": "string",
    "tinyint": "byte",
    "smallint": "short",
    "int": "integer",
    "integer": "integer",
    "bigint": "long",
    "float": "float",
    "double": "double",
    "decimal": "decimal",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "binary": "binary",
    "array": "array",
    "map": "map",
    "struct": "struct",
}


def _normalize_glue_type(glue_type: str) -> str:
    """Normalize Glue type string for mapping.

    Handles parameterized types like decimal(10,2), varchar(255), etc.
    """
    glue_type = glue_type.lower().strip()

    if "(" in glue_type:
        base_type = glue_type.split("(")[0]
        if base_type == "decimal":
            return glue_type
        return base_type

    return glue_type


def _map_glue_to_delta_type(glue_type: str) -> str:
    """Map a single Glue type to Delta type."""
    normalized = _normalize_glue_type(glue_type)

    if normalized.startswith("decimal"):
        return normalized

    return GLUE_TO_DELTA_TYPE_MAP.get(normalized, "string")


def build_delta_schema_from_glue(glue_columns: list[dict[str, str]]) -> dict[str, Any]:
    """Build Delta schema from Glue column definitions.

    Args:
        glue_columns: List of column dicts with 'Name'/'name' and 'Type'/'type' keys.

    Returns:
        Delta schema dict with 'type': 'struct' and 'fields' list.

    Raises:
        ValueError: If no columns provided or column has empty name.
    """
    if not glue_columns:
        raise ValueError("Invalid Glue schema: no columns provided")

    fields = []

    for col in glue_columns:
        col_name = col.get("Name", col.get("name", ""))
        if not col_name or col_name.strip() == "":
            raise ValueError(
                f"Invalid Glue schema: column without name found in schema: {col}"
            )

        col_type = col.get("Type", col.get("type", "string"))
        delta_type = _map_glue_to_delta_type(col_type)

        fields.append({
            "name": col_name,
            "type": delta_type,
            "nullable": True,
            "metadata": {},
        })

    return {"type": "struct", "fields": fields}


def build_delta_schema_from_spark(spark_schema) -> dict[str, Any]:
    """Build Delta schema from a Spark StructType.

    Spark's simpleString() produces Delta-compatible type strings directly
    (string, long, integer, double, decimal(10,2), array<string>, etc.).

    Args:
        spark_schema: A pyspark.sql.types.StructType (or mock with .fields).

    Returns:
        Delta schema dict with 'type': 'struct' and 'fields' list.

    Raises:
        ValueError: If schema has no fields.
    """
    if not spark_schema.fields:
        raise ValueError("Invalid Spark schema: no fields")

    fields = []
    for field in spark_schema.fields:
        fields.append({
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
            "metadata": {},
        })

    return {"type": "struct", "fields": fields}
```

**Step 4: Run test to verify it passes**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_schema.py -v`
Expected: PASS

**Step 5: Update delta_log.py to use schema.py**

Remove from `delta_log.py`:
- `GLUE_TO_DELTA_TYPE_MAP` dict (lines 21-46)
- `_normalize_glue_type()` function (lines 49-64)
- `_map_glue_to_delta_type()` function (lines 67-75)
- `build_delta_schema()` function (lines 78-115)

Add backward-compatible re-export at the top of `delta_log.py`:

```python
# Backward compatibility — schema functions moved to schema.py
from hive_to_delta.schema import (
    build_delta_schema_from_glue as build_delta_schema,
    _normalize_glue_type,
    _map_glue_to_delta_type,
    GLUE_TO_DELTA_TYPE_MAP,
)
```

**Step 6: Run ALL existing tests to verify no regressions**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_delta_log.py tests/test_schema.py -v`
Expected: ALL PASS — existing tests import from delta_log.py which re-exports from schema.py

**Step 7: Commit**

```bash
git add hive_to_delta/schema.py hive_to_delta/delta_log.py tests/test_schema.py
git commit -m "Extract schema inference to schema.py, add Spark schema builder"
```

---

### Task 3: Create discovery.py with protocols and GlueDiscovery

**Files:**
- Create: `hive_to_delta/discovery.py`
- Test: `tests/test_discovery.py` (create)

**Step 1: Write the failing tests**

Create `tests/test_discovery.py`:

```python
"""Unit tests for discovery strategies."""

import pytest
from unittest.mock import patch, MagicMock

from hive_to_delta.discovery import GlueDiscovery
from hive_to_delta.models import TableInfo


class TestGlueDiscovery:
    """Tests for GlueDiscovery strategy."""

    @patch("hive_to_delta.discovery.get_glue_table_metadata")
    @patch("hive_to_delta.discovery.list_glue_tables")
    def test_discover_single_table(self, mock_list, mock_metadata):
        """Test discovering a single table by exact name."""
        mock_list.return_value = ["events"]
        mock_metadata.return_value = {
            "StorageDescriptor": {
                "Location": "s3://bucket/events/",
                "Columns": [
                    {"Name": "id", "Type": "bigint"},
                    {"Name": "value", "Type": "string"},
                ],
            },
            "PartitionKeys": [{"Name": "year", "Type": "string"}],
        }

        discovery = GlueDiscovery(database="my_db", pattern="events", region="us-east-1")
        tables = discovery.discover(spark=None)

        assert len(tables) == 1
        assert isinstance(tables[0], TableInfo)
        assert tables[0].name == "events"
        assert tables[0].location == "s3://bucket/events"
        assert tables[0].partition_keys == ["year"]
        assert len(tables[0].columns) == 3  # 2 data + 1 partition

    @patch("hive_to_delta.discovery.get_glue_table_metadata")
    @patch("hive_to_delta.discovery.list_glue_tables")
    def test_discover_all_tables(self, mock_list, mock_metadata):
        """Test discovering all tables (no pattern)."""
        mock_list.return_value = ["t1", "t2"]
        mock_metadata.side_effect = [
            {
                "StorageDescriptor": {
                    "Location": "s3://bucket/t1",
                    "Columns": [{"Name": "id", "Type": "bigint"}],
                },
                "PartitionKeys": [],
            },
            {
                "StorageDescriptor": {
                    "Location": "s3://bucket/t2",
                    "Columns": [{"Name": "id", "Type": "bigint"}],
                },
                "PartitionKeys": [],
            },
        ]

        discovery = GlueDiscovery(database="my_db")
        tables = discovery.discover(spark=None)

        assert len(tables) == 2
        mock_list.assert_called_once_with("my_db", pattern=None, region="us-east-1")

    @patch("hive_to_delta.discovery.get_glue_table_metadata")
    @patch("hive_to_delta.discovery.list_glue_tables")
    def test_discover_with_pattern(self, mock_list, mock_metadata):
        """Test discovering tables matching a glob pattern."""
        mock_list.return_value = ["dim_customer", "dim_product"]
        mock_metadata.side_effect = [
            {
                "StorageDescriptor": {
                    "Location": "s3://bucket/dim_customer",
                    "Columns": [{"Name": "id", "Type": "bigint"}],
                },
                "PartitionKeys": [],
            },
            {
                "StorageDescriptor": {
                    "Location": "s3://bucket/dim_product",
                    "Columns": [{"Name": "id", "Type": "bigint"}],
                },
                "PartitionKeys": [],
            },
        ]

        discovery = GlueDiscovery(database="my_db", pattern="dim_*", region="us-west-2")
        tables = discovery.discover(spark=None)

        assert len(tables) == 2
        mock_list.assert_called_once_with("my_db", pattern="dim_*", region="us-west-2")

    @patch("hive_to_delta.discovery.list_glue_tables")
    def test_discover_no_matching_tables(self, mock_list):
        """Test discovery returns empty list when no tables match."""
        mock_list.return_value = []
        discovery = GlueDiscovery(database="my_db", pattern="nonexistent_*")
        tables = discovery.discover(spark=None)
        assert tables == []

    @patch("hive_to_delta.discovery.get_glue_table_metadata")
    @patch("hive_to_delta.discovery.list_glue_tables")
    def test_non_partitioned_table(self, mock_list, mock_metadata):
        """Test non-partitioned table has empty partition_keys."""
        mock_list.return_value = ["flat_table"]
        mock_metadata.return_value = {
            "StorageDescriptor": {
                "Location": "s3://bucket/flat",
                "Columns": [{"Name": "id", "Type": "bigint"}],
            },
            "PartitionKeys": [],
        }

        discovery = GlueDiscovery(database="my_db", pattern="flat_table")
        tables = discovery.discover(spark=None)

        assert tables[0].partition_keys == []
        assert len(tables[0].columns) == 1  # no partition keys to add
```

**Step 2: Run test to verify it fails**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_discovery.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write discovery.py implementation**

Create `hive_to_delta/discovery.py`:

```python
"""Table discovery strategies.

Discovery strategies determine which tables to convert and provide
their metadata (name, location, schema, partition keys).

Strategies:
- GlueDiscovery: Discover tables from AWS Glue Data Catalog
- UCDiscovery: Discover tables from Unity Catalog / hive_metastore
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, runtime_checkable

from hive_to_delta.glue import get_glue_table_metadata, list_glue_tables
from hive_to_delta.models import TableInfo


@runtime_checkable
class Discovery(Protocol):
    """Protocol for table discovery strategies."""

    def discover(self, spark: Any) -> list[TableInfo]:
        """Discover tables to convert.

        Args:
            spark: Active SparkSession (may be unused by some strategies).

        Returns:
            List of TableInfo with metadata for each discovered table.
        """
        ...


@dataclass
class GlueDiscovery:
    """Discover tables from AWS Glue Data Catalog.

    Wraps existing glue.py functions to produce TableInfo objects.

    Args:
        database: Glue database name.
        pattern: Optional glob pattern to filter table names.
        region: AWS region for Glue API calls.
    """

    database: str
    pattern: Optional[str] = None
    region: str = "us-east-1"

    def discover(self, spark: Any) -> list[TableInfo]:
        """Discover tables from Glue and return their metadata.

        For each table:
        - Fetches column definitions and partition keys from Glue
        - Combines data columns + partition key columns into columns list
        - Extracts S3 location from StorageDescriptor

        Args:
            spark: Not used by GlueDiscovery (Glue API is boto3-based).

        Returns:
            List of TableInfo with columns populated for Glue-based schema inference.
        """
        table_names = list_glue_tables(
            self.database, pattern=self.pattern, region=self.region
        )

        tables = []
        for name in table_names:
            metadata = get_glue_table_metadata(self.database, name, self.region)

            location = metadata["StorageDescriptor"]["Location"].rstrip("/")
            data_columns = metadata["StorageDescriptor"].get("Columns", [])
            partition_keys_meta = metadata.get("PartitionKeys", [])
            partition_key_names = [col["Name"] for col in partition_keys_meta]

            # Combine data columns + partition columns for full schema
            all_columns = data_columns + partition_keys_meta

            tables.append(
                TableInfo(
                    name=name,
                    location=location,
                    columns=all_columns,
                    partition_keys=partition_key_names,
                )
            )

        return tables
```

**Step 4: Run test to verify it passes**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_discovery.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add hive_to_delta/discovery.py tests/test_discovery.py
git commit -m "Add discovery module with GlueDiscovery strategy"
```

---

### Task 4: Create listing.py with S3Listing and InventoryListing

**Files:**
- Create: `hive_to_delta/listing.py`
- Test: `tests/test_listing.py` (create)

**Step 1: Write the failing tests**

Create `tests/test_listing.py`:

```python
"""Unit tests for file listing strategies."""

import pytest
from unittest.mock import patch, MagicMock, PropertyMock

from hive_to_delta.listing import S3Listing, InventoryListing
from hive_to_delta.models import TableInfo, ParquetFileInfo


class TestS3Listing:
    """Tests for S3Listing strategy (boto3-based)."""

    @patch("hive_to_delta.listing.get_glue_partitions")
    @patch("hive_to_delta.listing.scan_partition_files")
    def test_partitioned_table(self, mock_scan, mock_partitions):
        """Test listing files for a partitioned table."""
        mock_partitions.return_value = [
            {
                "Values": ["2024"],
                "StorageDescriptor": {"Location": "s3://bucket/table/year=2024"},
            },
            {
                "Values": ["2023"],
                "StorageDescriptor": {"Location": "s3://bucket/table/year=2023"},
            },
        ]
        mock_scan.return_value = [
            ParquetFileInfo("s3://bucket/table/year=2024/f1.parquet", 1024, {"year": "2024"}),
            ParquetFileInfo("s3://bucket/table/year=2023/f2.parquet", 2048, {"year": "2023"}),
        ]

        table = TableInfo(
            name="events",
            location="s3://bucket/table",
            partition_keys=["year"],
        )
        listing = S3Listing(region="us-east-1")
        files = listing.list_files(spark=None, table=table)

        assert len(files) == 2
        mock_scan.assert_called_once()
        # Verify partition locations were built correctly
        call_args = mock_scan.call_args[0][0]
        assert len(call_args) == 2
        assert call_args[0] == ("s3://bucket/table/year=2024", {"year": "2024"})

    @patch("hive_to_delta.listing.get_glue_partitions")
    @patch("hive_to_delta.listing.scan_partition_files")
    def test_non_partitioned_table(self, mock_scan, mock_partitions):
        """Test listing files for a non-partitioned table."""
        mock_partitions.return_value = []
        mock_scan.return_value = [
            ParquetFileInfo("s3://bucket/table/f1.parquet", 1024, {}),
        ]

        table = TableInfo(name="flat", location="s3://bucket/table", partition_keys=[])
        listing = S3Listing(region="us-east-1")
        files = listing.list_files(spark=None, table=table)

        assert len(files) == 1
        # Non-partitioned should scan table root
        call_args = mock_scan.call_args[0][0]
        assert call_args == [("s3://bucket/table", {})]


class TestInventoryListing:
    """Tests for InventoryListing strategy (DataFrame-based)."""

    def _make_mock_df(self, rows, columns=None, dtypes=None):
        """Create a mock DataFrame.

        Args:
            rows: List of tuples (file_path, size).
            columns: Column names (default: ["file_path", "size"]).
            dtypes: Column dtypes (default: [("file_path", "string"), ("size", "bigint")]).
        """
        df = MagicMock()
        df.columns = columns or ["file_path", "size"]
        df.dtypes = dtypes or [("file_path", "string"), ("size", "bigint")]

        # Mock collect() to return Row-like objects
        mock_rows = []
        for file_path, size in rows:
            row = MagicMock()
            row.file_path = file_path
            row.size = size
            row.__getitem__ = lambda self, key, fp=file_path, sz=size: (
                fp if key == "file_path" or key == 0 else sz
            )
            mock_rows.append(row)
        df.collect.return_value = mock_rows
        return df

    def test_basic_listing(self):
        """Test basic inventory listing produces ParquetFileInfo."""
        df = self._make_mock_df([
            ("s3://bucket/data/f1.parquet", 1024),
            ("s3://bucket/data/f2.parquet", 2048),
        ])

        table = TableInfo(name="events", location="s3://bucket/data")
        listing = InventoryListing(files_df=df)
        files = listing.list_files(spark=None, table=table)

        assert len(files) == 2
        assert files[0].path == "s3://bucket/data/f1.parquet"
        assert files[0].size == 1024
        assert files[0].partition_values == {}

    def test_with_partition_columns(self):
        """Test inventory listing parses partition values from paths."""
        df = self._make_mock_df([
            ("s3://bucket/data/year=2024/month=01/f1.parquet", 1024),
            ("s3://bucket/data/year=2024/month=02/f2.parquet", 2048),
        ])

        table = TableInfo(
            name="events",
            location="s3://bucket/data",
            partition_keys=["year", "month"],
        )
        listing = InventoryListing(files_df=df)
        files = listing.list_files(spark=None, table=table)

        assert files[0].partition_values == {"year": "2024", "month": "01"}
        assert files[1].partition_values == {"year": "2024", "month": "02"}

    def test_missing_file_path_column_raises(self):
        """Test validation rejects DataFrame without file_path column."""
        df = self._make_mock_df([], columns=["path", "size"],
                                dtypes=[("path", "string"), ("size", "bigint")])

        with pytest.raises(ValueError, match="file_path"):
            InventoryListing(files_df=df)

    def test_missing_size_column_raises(self):
        """Test validation rejects DataFrame without size column."""
        df = self._make_mock_df([], columns=["file_path", "length"],
                                dtypes=[("file_path", "string"), ("length", "bigint")])

        with pytest.raises(ValueError, match="size"):
            InventoryListing(files_df=df)

    def test_wrong_file_path_type_raises(self):
        """Test validation rejects non-string file_path column."""
        df = self._make_mock_df([], columns=["file_path", "size"],
                                dtypes=[("file_path", "int"), ("size", "bigint")])

        with pytest.raises(ValueError, match="file_path.*string"):
            InventoryListing(files_df=df)

    def test_wrong_size_type_raises(self):
        """Test validation rejects non-numeric size column."""
        df = self._make_mock_df([], columns=["file_path", "size"],
                                dtypes=[("file_path", "string"), ("size", "string")])

        with pytest.raises(ValueError, match="size.*int|long|bigint"):
            InventoryListing(files_df=df)

    def test_partition_value_not_in_path_skipped(self):
        """Test that missing partition key in path results in empty string value."""
        df = self._make_mock_df([
            ("s3://bucket/data/year=2024/f1.parquet", 1024),
        ])

        table = TableInfo(
            name="events",
            location="s3://bucket/data",
            partition_keys=["year", "region"],  # region not in path
        )
        listing = InventoryListing(files_df=df)
        files = listing.list_files(spark=None, table=table)

        assert files[0].partition_values["year"] == "2024"
        assert files[0].partition_values.get("region", "") == ""
```

**Step 2: Run test to verify it fails**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_listing.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write listing.py implementation**

Create `hive_to_delta/listing.py`:

```python
"""File listing strategies.

Listing strategies enumerate parquet files for a given table.

Strategies:
- S3Listing: List files via boto3 S3 API (requires AWS credentials)
- InventoryListing: Use a pre-built DataFrame of file metadata (e.g., from S3 Inventory)
- DatabricksListing: Use Databricks auth for file listing (future - research needed)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional, Protocol, runtime_checkable

from hive_to_delta.glue import get_glue_partitions
from hive_to_delta.models import ParquetFileInfo, TableInfo
from hive_to_delta.s3 import scan_partition_files


@runtime_checkable
class Listing(Protocol):
    """Protocol for file listing strategies."""

    def list_files(self, spark: Any, table: TableInfo) -> list[ParquetFileInfo]:
        """List parquet files for a table.

        Args:
            spark: Active SparkSession (may be unused by some strategies).
            table: Table metadata from a discovery strategy.

        Returns:
            List of ParquetFileInfo for each discovered parquet file.
        """
        ...


@dataclass
class S3Listing:
    """List parquet files via boto3 S3 API.

    For partitioned tables, fetches partition info from Glue and scans
    each partition location. For non-partitioned tables, scans the table root.

    Note: This strategy requires both AWS credentials and Glue access
    for partition enumeration on partitioned tables.

    Args:
        region: AWS region for S3/Glue API calls.
        glue_database: Glue database name (needed for partition lookups).
    """

    region: str = "us-east-1"
    glue_database: Optional[str] = None

    def list_files(self, spark: Any, table: TableInfo) -> list[ParquetFileInfo]:
        """List parquet files using S3 API.

        For partitioned tables (table.partition_keys is non-empty), fetches
        partition locations from Glue and scans each. For non-partitioned
        tables, scans the table root directly.

        Args:
            spark: Not used (S3 listing is boto3-based).
            table: Table metadata with location and partition_keys.

        Returns:
            List of ParquetFileInfo with partition_values populated.
        """
        if table.partition_keys and self.glue_database:
            # Partitioned table — get partition info from Glue
            partitions = get_glue_partitions(
                self.glue_database, table.name, self.region
            )

            if partitions:
                partition_locations = []
                for partition in partitions:
                    location = partition["StorageDescriptor"]["Location"]
                    values = partition["Values"]
                    partition_values = dict(zip(table.partition_keys, values))
                    partition_locations.append((location, partition_values))
            else:
                # Has partition keys but no partitions found — scan root
                partition_locations = [(table.location, {})]
        else:
            # Non-partitioned table — scan table root
            partition_locations = [(table.location, {})]

        return scan_partition_files(partition_locations, self.region)


def _parse_partition_values(file_path: str, partition_keys: list[str]) -> dict[str, str]:
    """Parse partition values from Hive-style key=value/ path segments.

    Args:
        file_path: Full S3 file path.
        partition_keys: Expected partition column names.

    Returns:
        Dict mapping partition key names to their values.
        Missing keys map to empty string.
    """
    # Extract all key=value pairs from path
    pairs = re.findall(r"([^/]+)=([^/]+)", file_path)
    path_partitions = dict(pairs)

    return {key: path_partitions.get(key, "") for key in partition_keys}


class InventoryListing:
    """List parquet files from a user-provided DataFrame.

    Accepts a Spark DataFrame with file metadata (e.g., from S3 Inventory).
    Required columns:
    - file_path (string): Full S3 URI
    - size (long/int/bigint): File size in bytes

    Args:
        files_df: Spark DataFrame with file_path and size columns.

    Raises:
        ValueError: If DataFrame is missing required columns or has wrong types.
    """

    def __init__(self, files_df: Any) -> None:
        self._validate_dataframe(files_df)
        self.files_df = files_df

    @staticmethod
    def _validate_dataframe(df: Any) -> None:
        """Validate DataFrame has required columns with correct types."""
        columns = set(df.columns)

        if "file_path" not in columns:
            raise ValueError(
                "DataFrame must contain a 'file_path' column. "
                f"Found columns: {sorted(columns)}"
            )
        if "size" not in columns:
            raise ValueError(
                "DataFrame must contain a 'size' column. "
                f"Found columns: {sorted(columns)}"
            )

        # Check types
        dtype_map = dict(df.dtypes)
        if dtype_map["file_path"] != "string":
            raise ValueError(
                f"Column 'file_path' must be string type, got '{dtype_map['file_path']}'"
            )
        if dtype_map["size"] not in ("int", "bigint", "long"):
            raise ValueError(
                f"Column 'size' must be int/long/bigint type, got '{dtype_map['size']}'"
            )

    def list_files(self, spark: Any, table: TableInfo) -> list[ParquetFileInfo]:
        """Collect DataFrame rows into ParquetFileInfo objects.

        If the table has partition_keys, parses key=value/ segments from
        each file_path to populate partition_values.

        Args:
            spark: Not used (DataFrame already in memory).
            table: Table metadata, used for partition_keys.

        Returns:
            List of ParquetFileInfo.
        """
        rows = self.files_df.collect()
        files = []

        for row in rows:
            file_path = row.file_path
            size = row.size

            if table.partition_keys:
                partition_values = _parse_partition_values(file_path, table.partition_keys)
            else:
                partition_values = {}

            files.append(
                ParquetFileInfo(
                    path=file_path,
                    size=size,
                    partition_values=partition_values,
                )
            )

        return files
```

**Step 4: Run test to verify it passes**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_listing.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add hive_to_delta/listing.py tests/test_listing.py
git commit -m "Add listing module with S3Listing and InventoryListing strategies"
```

---

### Task 5: Refactor converter.py — shared pipeline + two-tier API

**Files:**
- Modify: `hive_to_delta/converter.py`
- Test: `tests/test_converter.py` (create)

**Step 1: Write failing tests for the new API**

Create `tests/test_converter.py`:

```python
"""Unit tests for the refactored converter module."""

import pytest
from unittest.mock import patch, MagicMock, PropertyMock

from hive_to_delta.converter import convert_table, convert, _convert_one_table
from hive_to_delta.models import ConversionResult, ParquetFileInfo, TableInfo


class TestConvertOneTable:
    """Tests for the shared internal pipeline."""

    @patch("hive_to_delta.converter.write_delta_log")
    @patch("hive_to_delta.converter.generate_delta_log")
    @patch("hive_to_delta.converter.build_delta_schema_from_glue")
    def test_with_glue_columns(self, mock_schema, mock_gen, mock_write, ):
        """Test _convert_one_table uses Glue schema when columns are provided."""
        mock_schema.return_value = {"type": "struct", "fields": []}
        mock_gen.return_value = '{"protocol":{}}'
        mock_write.return_value = "s3://bucket/t/_delta_log/00000000000000000000.json"

        spark = MagicMock()
        table = TableInfo(
            name="events",
            location="s3://bucket/t",
            columns=[{"Name": "id", "Type": "bigint"}],
        )
        files = [ParquetFileInfo("s3://bucket/t/f.parquet", 1024)]

        result = _convert_one_table(
            spark=spark,
            table_info=table,
            files=files,
            target_catalog="cat",
            target_schema="sch",
            aws_region="us-east-1",
        )

        assert result.success
        assert result.file_count == 1
        mock_schema.assert_called_once()

    @patch("hive_to_delta.converter.write_delta_log")
    @patch("hive_to_delta.converter.generate_delta_log")
    @patch("hive_to_delta.converter.build_delta_schema_from_spark")
    def test_with_spark_inference(self, mock_spark_schema, mock_gen, mock_write):
        """Test _convert_one_table infers schema from parquet when no columns."""
        mock_spark_schema.return_value = {"type": "struct", "fields": []}
        mock_gen.return_value = '{"protocol":{}}'
        mock_write.return_value = "s3://bucket/t/_delta_log/00000000000000000000.json"

        spark = MagicMock()
        # Mock spark.read.parquet().schema
        mock_schema_obj = MagicMock()
        spark.read.parquet.return_value.schema = mock_schema_obj

        table = TableInfo(
            name="events",
            location="s3://bucket/t",
            columns=None,  # No Glue columns — triggers Spark inference
        )
        files = [ParquetFileInfo("s3://bucket/t/f.parquet", 1024)]

        result = _convert_one_table(
            spark=spark,
            table_info=table,
            files=files,
            target_catalog="cat",
            target_schema="sch",
            aws_region="us-east-1",
        )

        assert result.success
        spark.read.parquet.assert_called_once_with("s3://bucket/t/f.parquet")
        mock_spark_schema.assert_called_once_with(mock_schema_obj)

    @patch("hive_to_delta.converter.write_delta_log")
    @patch("hive_to_delta.converter.generate_delta_log")
    @patch("hive_to_delta.converter.build_delta_schema_from_glue")
    def test_creates_schema_and_table(self, mock_schema, mock_gen, mock_write):
        """Test that SQL statements for schema and table creation are executed."""
        mock_schema.return_value = {"type": "struct", "fields": []}
        mock_gen.return_value = '{"protocol":{}}'
        mock_write.return_value = "s3://bucket/t/_delta_log/0.json"

        spark = MagicMock()
        table = TableInfo(name="events", location="s3://bucket/t",
                         columns=[{"Name": "id", "Type": "bigint"}])
        files = [ParquetFileInfo("s3://bucket/t/f.parquet", 1024)]

        _convert_one_table(spark, table, files, "cat", "sch", "us-east-1")

        # Verify SQL calls
        sql_calls = [call[0][0] for call in spark.sql.call_args_list]
        assert any("CREATE SCHEMA IF NOT EXISTS" in s for s in sql_calls)
        assert any("CREATE TABLE" in s for s in sql_calls)
        assert any("USING DELTA" in s for s in sql_calls)

    def test_no_files_returns_failure(self):
        """Test that empty files list returns a failed result."""
        spark = MagicMock()
        table = TableInfo(name="events", location="s3://bucket/t")

        result = _convert_one_table(spark, table, [], "cat", "sch", "us-east-1")

        assert not result.success
        assert "No parquet files" in result.error

    @patch("hive_to_delta.converter.write_delta_log")
    @patch("hive_to_delta.converter.generate_delta_log")
    @patch("hive_to_delta.converter.build_delta_schema_from_glue")
    def test_target_table_name_override(self, mock_schema, mock_gen, mock_write):
        """Test target_table_name override is used."""
        mock_schema.return_value = {"type": "struct", "fields": []}
        mock_gen.return_value = '{"protocol":{}}'
        mock_write.return_value = "s3://bucket/t/_delta_log/0.json"

        spark = MagicMock()
        table = TableInfo(
            name="events",
            location="s3://bucket/t",
            target_table_name="raw_events",
            columns=[{"Name": "id", "Type": "bigint"}],
        )
        files = [ParquetFileInfo("s3://bucket/t/f.parquet", 1024)]

        result = _convert_one_table(spark, table, files, "cat", "sch", "us-east-1")

        assert result.target_table == "cat.sch.raw_events"


class TestConvertTable:
    """Tests for the simple convert_table() entry point."""

    @patch("hive_to_delta.converter._convert_one_table")
    def test_basic_call(self, mock_convert):
        """Test convert_table collects DataFrame and calls pipeline."""
        mock_convert.return_value = ConversionResult(
            source_table="events",
            target_table="cat.sch.events",
            success=True,
            file_count=2,
        )

        # Mock DataFrame
        df = MagicMock()
        df.columns = ["file_path", "size"]
        df.dtypes = [("file_path", "string"), ("size", "bigint")]
        row1 = MagicMock()
        row1.file_path = "s3://bucket/f1.parquet"
        row1.size = 1024
        row2 = MagicMock()
        row2.file_path = "s3://bucket/f2.parquet"
        row2.size = 2048
        df.collect.return_value = [row1, row2]

        spark = MagicMock()
        result = convert_table(
            spark=spark,
            files_df=df,
            table_location="s3://bucket/data",
            target_catalog="cat",
            target_schema="sch",
            target_table="events",
        )

        assert result.success
        mock_convert.assert_called_once()
        # Verify files were collected
        call_args = mock_convert.call_args
        assert len(call_args[1]["files"]) == 2 or len(call_args[0][2]) == 2

    @patch("hive_to_delta.converter._convert_one_table")
    def test_with_partition_columns(self, mock_convert):
        """Test convert_table passes partition_columns through."""
        mock_convert.return_value = ConversionResult(
            source_table="events", target_table="cat.sch.events",
            success=True, file_count=1,
        )

        df = MagicMock()
        df.columns = ["file_path", "size"]
        df.dtypes = [("file_path", "string"), ("size", "bigint")]
        row = MagicMock()
        row.file_path = "s3://bucket/year=2024/f1.parquet"
        row.size = 1024
        df.collect.return_value = [row]

        spark = MagicMock()
        convert_table(
            spark=spark,
            files_df=df,
            table_location="s3://bucket/data",
            target_catalog="cat",
            target_schema="sch",
            target_table="events",
            partition_columns=["year"],
        )

        # Verify TableInfo has partition_keys
        call_args = mock_convert.call_args
        table_info = call_args[1].get("table_info") or call_args[0][1]
        assert table_info.partition_keys == ["year"]

    def test_invalid_dataframe_raises(self):
        """Test convert_table validates DataFrame schema."""
        df = MagicMock()
        df.columns = ["path", "size"]  # Wrong column name
        df.dtypes = [("path", "string"), ("size", "bigint")]

        with pytest.raises(ValueError, match="file_path"):
            convert_table(
                spark=MagicMock(),
                files_df=df,
                table_location="s3://bucket/data",
                target_catalog="cat",
                target_schema="sch",
                target_table="events",
            )


class TestConvert:
    """Tests for the composable convert() entry point."""

    @patch("hive_to_delta.converter._convert_one_table")
    def test_discovery_and_listing(self, mock_convert):
        """Test convert() calls discovery.discover and listing.list_files."""
        mock_convert.return_value = ConversionResult(
            source_table="t1", target_table="cat.sch.t1", success=True, file_count=1,
        )

        discovery = MagicMock()
        discovery.discover.return_value = [
            TableInfo(name="t1", location="s3://bucket/t1"),
        ]

        listing = MagicMock()
        listing.list_files.return_value = [
            ParquetFileInfo("s3://bucket/t1/f.parquet", 1024),
        ]

        spark = MagicMock()
        results = convert(
            spark=spark,
            discovery=discovery,
            listing=listing,
            target_catalog="cat",
            target_schema="sch",
        )

        assert len(results) == 1
        assert results[0].success
        discovery.discover.assert_called_once_with(spark)
        listing.list_files.assert_called_once()

    @patch("hive_to_delta.converter.run_parallel")
    def test_parallel_execution(self, mock_parallel):
        """Test convert() uses run_parallel for multiple tables."""
        mock_parallel.return_value = [
            ConversionResult(source_table="t1", target_table="cat.sch.t1", success=True),
            ConversionResult(source_table="t2", target_table="cat.sch.t2", success=True),
        ]

        discovery = MagicMock()
        discovery.discover.return_value = [
            TableInfo(name="t1", location="s3://b/t1"),
            TableInfo(name="t2", location="s3://b/t2"),
        ]

        listing = MagicMock()
        listing.list_files.return_value = [
            ParquetFileInfo("s3://b/t1/f.parquet", 1024),
        ]

        spark = MagicMock()
        results = convert(
            spark=spark,
            discovery=discovery,
            listing=listing,
            target_catalog="cat",
            target_schema="sch",
            max_workers=4,
        )

        assert len(results) == 2

    def test_empty_discovery_returns_empty(self):
        """Test convert() returns empty list when discovery finds nothing."""
        discovery = MagicMock()
        discovery.discover.return_value = []

        listing = MagicMock()
        spark = MagicMock()

        results = convert(
            spark=spark,
            discovery=discovery,
            listing=listing,
            target_catalog="cat",
            target_schema="sch",
        )

        assert results == []
        listing.list_files.assert_not_called()
```

**Step 2: Run test to verify it fails**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_converter.py -v`
Expected: FAIL — `ImportError: cannot import name 'convert_table' from 'hive_to_delta.converter'`

**Step 3: Rewrite converter.py**

Replace `hive_to_delta/converter.py` with the new implementation. The key changes:

1. Add `_convert_one_table()` — shared pipeline extracted from `convert_single_table()`
2. Add `convert_table()` — simple entry point using InventoryListing internally
3. Add `convert()` — composable entry point accepting Discovery + Listing strategies
4. Keep `convert_single_table()` and `convert_tables()` as backward-compatible wrappers

```python
"""Converter module — orchestrates table conversion.

Two-tier API:
- convert_table(): Simple, manual, single-table conversion from a DataFrame
- convert(): Composable bulk conversion with pluggable discovery + listing strategies

Internal:
- _convert_one_table(): Shared pipeline (schema → delta log → register)

Legacy (backward compatible):
- convert_single_table(): Original Glue-based single table conversion
- convert_tables(): Original Glue-based bulk conversion
"""

import time
from typing import Any, Optional, Union

from hive_to_delta.delta_log import generate_delta_log, write_delta_log
from hive_to_delta.listing import InventoryListing, Listing, _parse_partition_values
from hive_to_delta.models import ConversionResult, ParquetFileInfo, TableInfo
from hive_to_delta.parallel import ConversionSummary, create_summary, run_parallel
from hive_to_delta.schema import build_delta_schema_from_glue, build_delta_schema_from_spark


# ---------------------------------------------------------------------------
# Shared internal pipeline
# ---------------------------------------------------------------------------


def _convert_one_table(
    spark: Any,
    table_info: TableInfo,
    files: list[ParquetFileInfo],
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
) -> ConversionResult:
    """Convert a single table's parquet files to Delta and register in UC.

    This is the shared pipeline used by both convert_table() and convert().

    Steps:
    1. Infer Delta schema (from Glue columns or Spark parquet read)
    2. Generate Delta transaction log
    3. Write Delta log to S3
    4. Create schema in Unity Catalog (if needed)
    5. Register table in Unity Catalog

    Args:
        spark: Active SparkSession with UC access.
        table_info: Table metadata (name, location, columns, partition_keys).
        files: List of parquet files to include in the Delta table.
        target_catalog: Unity Catalog catalog name.
        target_schema: Unity Catalog schema name.
        aws_region: AWS region for S3 operations.

    Returns:
        ConversionResult with success/failure details.
    """
    start_time = time.perf_counter()
    final_table_name = table_info.target_table_name or table_info.name
    target_table = f"{target_catalog}.{target_schema}.{final_table_name}"

    try:
        if not files:
            duration = time.perf_counter() - start_time
            return ConversionResult(
                source_table=table_info.name,
                target_table=target_table,
                success=False,
                error="No parquet files found for table",
                duration_seconds=duration,
            )

        # Step 1: Infer schema
        if table_info.columns is not None:
            # Glue-based schema inference
            schema = build_delta_schema_from_glue(table_info.columns)
        else:
            # Spark-based schema inference — read one parquet file
            sample_path = files[0].path
            spark_schema = spark.read.parquet(sample_path).schema
            schema = build_delta_schema_from_spark(spark_schema)

        # Step 2: Generate Delta transaction log
        delta_log_content = generate_delta_log(
            files=files,
            schema=schema,
            partition_columns=table_info.partition_keys,
            table_location=table_info.location,
        )

        # Step 3: Write Delta log to S3
        delta_log_path = write_delta_log(
            delta_log_content, table_info.location, aws_region
        )

        # Step 4: Ensure schema exists
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

        # Step 5: Register table in Unity Catalog
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")
        spark.sql(f"""
            CREATE TABLE {target_table}
            USING DELTA
            LOCATION '{table_info.location}'
        """)

        duration = time.perf_counter() - start_time
        return ConversionResult(
            source_table=table_info.name,
            target_table=target_table,
            success=True,
            file_count=len(files),
            delta_log_location=delta_log_path,
            duration_seconds=duration,
        )

    except Exception as e:
        duration = time.perf_counter() - start_time
        return ConversionResult(
            source_table=table_info.name,
            target_table=target_table,
            success=False,
            error=str(e),
            duration_seconds=duration,
        )


# ---------------------------------------------------------------------------
# Tier 1: Simple / Manual API
# ---------------------------------------------------------------------------


def convert_table(
    spark: Any,
    files_df: Any,
    table_location: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    partition_columns: Optional[list[str]] = None,
    aws_region: str = "us-east-1",
) -> ConversionResult:
    """Convert parquet files from a DataFrame into a Delta table.

    The simplest entry point — provide a DataFrame of file metadata and
    get a registered Delta table in Unity Catalog.

    Args:
        spark: Active SparkSession with UC access.
        files_df: Spark DataFrame with columns:
            - file_path (string): Full S3 URI
            - size (long/int): File size in bytes
        table_location: S3 path where _delta_log/ will be written.
        target_catalog: Unity Catalog catalog name.
        target_schema: Unity Catalog schema name.
        target_table: Table name to register in UC.
        partition_columns: Optional list of partition column names.
            If provided, partition values are parsed from key=value/ path segments.
        aws_region: AWS region for S3 operations.

    Returns:
        ConversionResult with success/failure details.

    Raises:
        ValueError: If files_df is missing required columns or has wrong types.

    Example:
        >>> from hive_to_delta import convert_table
        >>> result = convert_table(
        ...     spark=spark,
        ...     files_df=inventory_df,
        ...     table_location="s3://my-bucket/data/events",
        ...     target_catalog="analytics",
        ...     target_schema="bronze",
        ...     target_table="events",
        ... )
    """
    # Validate DataFrame (raises ValueError on failure)
    InventoryListing._validate_dataframe(files_df)

    # Build TableInfo
    table_info = TableInfo(
        name=target_table,
        location=table_location.rstrip("/"),
        partition_keys=partition_columns or [],
    )

    # Collect DataFrame → ParquetFileInfo list
    rows = files_df.collect()
    files = []
    for row in rows:
        if partition_columns:
            partition_values = _parse_partition_values(row.file_path, partition_columns)
        else:
            partition_values = {}

        files.append(
            ParquetFileInfo(
                path=row.file_path,
                size=row.size,
                partition_values=partition_values,
            )
        )

    return _convert_one_table(
        spark=spark,
        table_info=table_info,
        files=files,
        target_catalog=target_catalog,
        target_schema=target_schema,
        aws_region=aws_region,
    )


# ---------------------------------------------------------------------------
# Tier 2: Composable / Bulk API
# ---------------------------------------------------------------------------


def convert(
    spark: Any,
    discovery: Any,  # Discovery protocol
    listing: Any,  # Listing protocol
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    max_workers: int = 4,
    print_summary: bool = True,
) -> list[ConversionResult]:
    """Convert tables using composable discovery and listing strategies.

    Discovers tables via the discovery strategy, enumerates files via
    the listing strategy, then converts each table in parallel.

    Args:
        spark: Active SparkSession with UC access.
        discovery: Discovery strategy (implements discover(spark) -> list[TableInfo]).
        listing: Listing strategy (implements list_files(spark, table) -> list[ParquetFileInfo]).
        target_catalog: Unity Catalog catalog name.
        target_schema: Unity Catalog schema name.
        aws_region: AWS region for S3 operations.
        max_workers: Maximum parallel conversion workers.
        print_summary: Print a summary after completion.

    Returns:
        List of ConversionResult for each table.

    Example:
        >>> from hive_to_delta import convert
        >>> from hive_to_delta.discovery import GlueDiscovery
        >>> from hive_to_delta.listing import S3Listing
        >>> results = convert(
        ...     spark=spark,
        ...     discovery=GlueDiscovery(database="my_db", pattern="dim_*"),
        ...     listing=S3Listing(region="us-east-1"),
        ...     target_catalog="analytics",
        ...     target_schema="bronze",
        ... )
    """
    # Step 1: Discover tables
    tables = discovery.discover(spark)

    if not tables:
        if print_summary:
            print("No tables found by discovery strategy")
        return []

    # Step 2: List files for each table
    table_files: list[tuple[TableInfo, list[ParquetFileInfo]]] = []
    for table in tables:
        files = listing.list_files(spark, table)
        table_files.append((table, files))

    # Step 3: Convert each table
    def convert_one(pair: tuple[TableInfo, list[ParquetFileInfo]]) -> ConversionResult:
        table_info, files = pair
        return _convert_one_table(
            spark=spark,
            table_info=table_info,
            files=files,
            target_catalog=target_catalog,
            target_schema=target_schema,
            aws_region=aws_region,
        )

    if len(table_files) == 1:
        # Single table — no need for parallelism
        results = [convert_one(table_files[0])]
    else:
        results_raw = run_parallel(convert_one, table_files, max_workers=max_workers)
        # Convert exceptions to failed ConversionResult
        results = []
        for i, result in enumerate(results_raw):
            if isinstance(result, Exception):
                table_info = table_files[i][0] if i < len(table_files) else None
                name = table_info.name if table_info else "unknown"
                results.append(
                    ConversionResult(
                        source_table=name,
                        target_table=f"{target_catalog}.{target_schema}.{name}",
                        success=False,
                        error=str(result),
                    )
                )
            else:
                results.append(result)

    if print_summary and len(results) > 1:
        summary = create_summary(results)
        print(summary)

    return results


# ---------------------------------------------------------------------------
# Legacy API (backward compatible)
# ---------------------------------------------------------------------------


def convert_single_table(
    spark: Any,
    glue_database: str,
    table_name: str,
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    target_table_name: Optional[str] = None,
) -> ConversionResult:
    """Convert a single Hive table to Delta and register in Unity Catalog.

    Legacy API — wraps the new composable pipeline with GlueDiscovery + S3Listing.
    For new code, prefer convert_table() or convert().

    Args:
        spark: Active Spark session with Unity Catalog access.
        glue_database: Name of the Glue database containing the source table.
        table_name: Name of the source table in Glue.
        target_catalog: Unity Catalog catalog for the target table.
        target_schema: Unity Catalog schema for the target table.
        aws_region: AWS region for Glue/S3 operations.
        target_table_name: Optional override for the target table name.

    Returns:
        ConversionResult with success/failure details.
    """
    from hive_to_delta.discovery import GlueDiscovery
    from hive_to_delta.listing import S3Listing

    # Use GlueDiscovery to get table metadata
    discovery = GlueDiscovery(
        database=glue_database, pattern=table_name, region=aws_region
    )
    tables = discovery.discover(spark)

    if not tables:
        return ConversionResult(
            source_table=table_name,
            target_table=f"{target_catalog}.{target_schema}.{target_table_name or table_name}",
            success=False,
            error=f"Table '{table_name}' not found in Glue database '{glue_database}'",
        )

    table_info = tables[0]
    if target_table_name:
        table_info.target_table_name = target_table_name

    # Use S3Listing to get files
    listing = S3Listing(region=aws_region, glue_database=glue_database)
    files = listing.list_files(spark, table_info)

    return _convert_one_table(
        spark=spark,
        table_info=table_info,
        files=files,
        target_catalog=target_catalog,
        target_schema=target_schema,
        aws_region=aws_region,
    )


def convert_tables(
    spark: Any,
    glue_database: str,
    tables: Union[list[str], str],
    target_catalog: str,
    target_schema: str,
    aws_region: str = "us-east-1",
    max_workers: int = 4,
    print_summary: bool = True,
) -> list[ConversionResult]:
    """Convert multiple Hive tables to Delta in parallel.

    Legacy API — wraps the new composable pipeline with GlueDiscovery + S3Listing.
    For new code, prefer convert().

    Args:
        spark: Active Spark session with Unity Catalog access.
        glue_database: Glue database name.
        tables: List of table names or glob pattern string.
        target_catalog: Unity Catalog catalog name.
        target_schema: Unity Catalog schema name.
        aws_region: AWS region.
        max_workers: Maximum parallel workers.
        print_summary: Print summary after completion.

    Returns:
        List of ConversionResult for each table.
    """
    from hive_to_delta.discovery import GlueDiscovery
    from hive_to_delta.listing import S3Listing

    # Determine pattern
    if isinstance(tables, str):
        pattern = tables
    else:
        pattern = None

    discovery = GlueDiscovery(database=glue_database, pattern=pattern, region=aws_region)
    listing = S3Listing(region=aws_region, glue_database=glue_database)

    if isinstance(tables, list):
        # Filter discovered tables to only those in the explicit list
        all_tables = discovery.discover(spark)
        discovered = [t for t in all_tables if t.name in tables]
        if not discovered:
            if print_summary:
                print(f"No tables found in {glue_database}")
            return []
    else:
        discovered = discovery.discover(spark)
        if not discovered:
            if print_summary:
                print(f"No tables found matching pattern '{tables}' in {glue_database}")
            return []
        if print_summary:
            print(f"Found {len(discovered)} tables matching pattern '{tables}'")

    # List files and convert
    table_files = []
    for table_info in discovered:
        files = listing.list_files(spark, table_info)
        table_files.append((table_info, files))

    def convert_one(pair):
        table_info, files = pair
        return _convert_one_table(spark, table_info, files, target_catalog, target_schema, aws_region)

    results_raw = run_parallel(convert_one, table_files, max_workers=max_workers)

    final_results = []
    for i, result in enumerate(results_raw):
        if isinstance(result, Exception):
            name = table_files[i][0].name if i < len(table_files) else "unknown"
            final_results.append(
                ConversionResult(
                    source_table=name,
                    target_table=f"{target_catalog}.{target_schema}.{name}",
                    success=False,
                    error=str(result),
                )
            )
        else:
            final_results.append(result)

    if print_summary:
        summary = create_summary(final_results)
        print(summary)

    return final_results
```

**Step 4: Run new tests**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_converter.py -v`
Expected: PASS

**Step 5: Run ALL existing tests to check backward compatibility**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_delta_log.py tests/test_schema.py tests/test_converter.py tests/test_models.py -v`
Expected: ALL PASS

**Step 6: Commit**

```bash
git add hive_to_delta/converter.py tests/test_converter.py
git commit -m "Refactor converter to composable pipeline with convert_table() and convert()"
```

---

### Task 6: Update __init__.py exports

**Files:**
- Modify: `hive_to_delta/__init__.py`

**Step 1: Update exports**

```python
"""
hive_to_delta - Convert Hive parquet tables to Delta and register in Unity Catalog.

Simple API:
    from hive_to_delta import convert_table
    result = convert_table(spark, files_df, table_location, target_catalog, target_schema, target_table)

Composable API:
    from hive_to_delta import convert
    from hive_to_delta.discovery import GlueDiscovery, UCDiscovery
    from hive_to_delta.listing import S3Listing, InventoryListing
    results = convert(spark, discovery=GlueDiscovery(...), listing=S3Listing(...), ...)

Legacy API (backward compatible):
    from hive_to_delta import convert_single_table, convert_tables
"""

from hive_to_delta.converter import (
    convert,
    convert_single_table,
    convert_table,
    convert_tables,
)
from hive_to_delta.discovery import Discovery, GlueDiscovery
from hive_to_delta.glue import list_glue_tables
from hive_to_delta.listing import InventoryListing, Listing, S3Listing
from hive_to_delta.models import ConversionResult, ParquetFileInfo, TableInfo
from hive_to_delta.parallel import ConversionSummary, create_summary
from hive_to_delta.vacuum import vacuum_external_files

__all__ = [
    # Tier 1: Simple API
    "convert_table",
    # Tier 2: Composable API
    "convert",
    "Discovery",
    "GlueDiscovery",
    "Listing",
    "S3Listing",
    "InventoryListing",
    # Models
    "ConversionResult",
    "ParquetFileInfo",
    "TableInfo",
    # Utilities
    "vacuum_external_files",
    "list_glue_tables",
    "ConversionSummary",
    "create_summary",
    # Legacy
    "convert_single_table",
    "convert_tables",
]
__version__ = "0.2.0"
```

**Step 2: Run import test**

Run: `cd databricks/hive_to_delta && python -c "from hive_to_delta import convert_table, convert, GlueDiscovery, S3Listing, InventoryListing, TableInfo; print('All imports OK')"`
Expected: `All imports OK`

**Step 3: Commit**

```bash
git add hive_to_delta/__init__.py
git commit -m "Update public API exports for composable pipeline"
```

---

### Task 7: Add UCDiscovery (research + implement)

This task requires Spark catalog APIs. The implementation will use `spark.sql()` for metadata queries.

**Files:**
- Modify: `hive_to_delta/discovery.py`
- Test: `tests/test_discovery.py` (extend)

**Step 1: Write failing tests for UCDiscovery**

Add to `tests/test_discovery.py`:

```python
from hive_to_delta.discovery import UCDiscovery


class TestUCDiscovery:
    """Tests for UCDiscovery strategy."""

    def _mock_spark_with_tables(self, tables_data):
        """Create a mock Spark session with catalog table data.

        tables_data: list of dicts with keys: database, tableName, catalog,
            location, columns (list of (name, type, is_partition))
        """
        spark = MagicMock()

        # Mock listDatabases/listTables - we use SQL instead for simplicity
        # Mock spark.sql() to return appropriate results for each query

        def sql_side_effect(query):
            result = MagicMock()
            q = query.strip().upper()

            if "SHOW TABLES" in q:
                # Return tables for the specified catalog.schema
                rows = []
                for t in tables_data:
                    row = MagicMock()
                    row.tableName = t["tableName"]
                    row.namespace = t.get("database", "default")
                    rows.append(row)
                result.collect.return_value = rows

            elif "DESCRIBE TABLE EXTENDED" in q:
                # Return table location
                matching = [t for t in tables_data if t["tableName"] in query]
                if matching:
                    t = matching[0]
                    rows = []
                    for key, val in [("Location", t["location"]), ("Provider", "hive")]:
                        row = MagicMock()
                        row.col_name = key
                        row.data_type = val
                        rows.append(row)
                    result.collect.return_value = rows

            return result

        spark.sql.side_effect = sql_side_effect

        # Mock spark.catalog.listColumns
        def list_columns_side_effect(table_name):
            for t in tables_data:
                fqn = f"{t['catalog']}.{t['database']}.{t['tableName']}"
                if fqn == table_name or t["tableName"] == table_name:
                    cols = []
                    for name, dtype, is_part in t["columns"]:
                        col = MagicMock()
                        col.name = name
                        col.dataType = dtype
                        col.isPartition = is_part
                        cols.append(col)
                    return cols
            return []

        spark.catalog.listColumns = list_columns_side_effect

        return spark

    def test_allow_list_filters_tables(self):
        """Test that allow list filters to matching FQNs."""
        discovery = UCDiscovery(allow=["hive_metastore.my_db.*"])

        # The actual filtering logic should work on FQN patterns
        assert discovery._matches_fqn("hive_metastore.my_db.events", discovery.allow)
        assert not discovery._matches_fqn("other_catalog.my_db.events", discovery.allow)

    def test_deny_list_excludes_tables(self):
        """Test that deny list excludes matching FQNs."""
        discovery = UCDiscovery(
            allow=["hive_metastore.*.*"],
            deny=["*.information_schema.*"],
        )

        assert not discovery._should_include("hive_metastore.information_schema.tables")
        assert discovery._should_include("hive_metastore.my_db.events")

    def test_default_deny_list(self):
        """Test default deny list excludes information_schema."""
        discovery = UCDiscovery(allow=["hive_metastore.*.*"])

        assert not discovery._should_include("hive_metastore.information_schema.columns")
        assert discovery._should_include("hive_metastore.default.my_table")
```

**Step 2: Run test to verify it fails**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_discovery.py::TestUCDiscovery -v`
Expected: FAIL — `ImportError: cannot import name 'UCDiscovery'`

**Step 3: Implement UCDiscovery**

Add to `hive_to_delta/discovery.py`:

```python
import fnmatch


@dataclass
class UCDiscovery:
    """Discover tables from Unity Catalog / hive_metastore.

    Uses Spark catalog APIs to enumerate tables and extract metadata.
    Works with any catalog accessible from the SparkSession, including
    hive_metastore for legacy Hive tables.

    Args:
        allow: Required list of FQN patterns (catalog.schema.table) to include.
            Supports glob patterns: "hive_metastore.my_db.*", "catalog.*.dim_*"
        deny: Optional list of FQN patterns to exclude.
            Defaults to ["*.information_schema.*"].
    """

    allow: list[str]
    deny: list[str] = field(default_factory=lambda: ["*.information_schema.*"])

    @staticmethod
    def _matches_fqn(fqn: str, patterns: list[str]) -> bool:
        """Check if a fully-qualified name matches any of the patterns."""
        return any(fnmatch.fnmatch(fqn, pattern) for pattern in patterns)

    def _should_include(self, fqn: str) -> bool:
        """Check if a FQN should be included (matches allow, not in deny)."""
        if not self._matches_fqn(fqn, self.allow):
            return False
        if self._matches_fqn(fqn, self.deny):
            return False
        return True

    def discover(self, spark: Any) -> list[TableInfo]:
        """Discover tables from UC catalogs using Spark catalog APIs.

        Parses the allow list to determine which catalog.schema combinations
        to enumerate, then filters individual tables against allow/deny lists.

        For each matching table:
        - Gets location via DESCRIBE TABLE EXTENDED
        - Gets partition columns via spark.catalog.listColumns (isPartition flag)
        - columns is set to None (schema inferred from parquet at conversion time)

        Args:
            spark: Active SparkSession with access to target catalogs.

        Returns:
            List of TableInfo (columns=None, partition_keys populated).
        """
        # Parse allow patterns to determine which catalog.schema pairs to enumerate
        catalog_schemas = set()
        for pattern in self.allow:
            parts = pattern.split(".")
            if len(parts) >= 2:
                catalog_schemas.add((parts[0], parts[1]))

        tables = []
        for catalog_pattern, schema_pattern in catalog_schemas:
            # Get matching catalogs
            try:
                catalog_rows = spark.sql("SHOW CATALOGS").collect()
                catalogs = [
                    r.catalog for r in catalog_rows
                    if fnmatch.fnmatch(r.catalog, catalog_pattern)
                ]
            except Exception:
                # Fallback: use the pattern literally
                catalogs = [catalog_pattern]

            for catalog in catalogs:
                # Get matching schemas
                try:
                    schema_rows = spark.sql(
                        f"SHOW SCHEMAS IN {catalog}"
                    ).collect()
                    schemas = [
                        r.databaseName for r in schema_rows
                        if fnmatch.fnmatch(r.databaseName, schema_pattern)
                    ]
                except Exception:
                    schemas = [schema_pattern]

                for schema in schemas:
                    # Get tables in this catalog.schema
                    try:
                        table_rows = spark.sql(
                            f"SHOW TABLES IN {catalog}.{schema}"
                        ).collect()
                    except Exception:
                        continue

                    for row in table_rows:
                        table_name = row.tableName
                        fqn = f"{catalog}.{schema}.{table_name}"

                        if not self._should_include(fqn):
                            continue

                        try:
                            # Get table location
                            desc_rows = spark.sql(
                                f"DESCRIBE TABLE EXTENDED {fqn}"
                            ).collect()
                            location = ""
                            for desc_row in desc_rows:
                                if desc_row.col_name == "Location":
                                    location = desc_row.data_type
                                    break

                            if not location:
                                continue

                            # Get partition columns
                            cols = spark.catalog.listColumns(fqn)
                            partition_keys = [
                                c.name for c in cols if c.isPartition
                            ]

                            tables.append(
                                TableInfo(
                                    name=table_name,
                                    location=location.rstrip("/"),
                                    columns=None,  # Infer from parquet
                                    partition_keys=partition_keys,
                                )
                            )
                        except Exception:
                            # Skip tables that can't be described
                            continue

        return tables
```

**Step 4: Run test to verify it passes**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_discovery.py -v`
Expected: PASS

**Step 5: Update __init__.py to export UCDiscovery**

Add `UCDiscovery` to imports and `__all__` in `__init__.py`.

**Step 6: Commit**

```bash
git add hive_to_delta/discovery.py hive_to_delta/__init__.py tests/test_discovery.py
git commit -m "Add UCDiscovery strategy for Unity Catalog table discovery"
```

---

### Task 8: Create research beads for deferred work

These are not implementation tasks — they document research questions for future sessions.

**Step 1: Create partition handling research bead**

```bash
bd create --title="Research: partition handling trade-offs for non-partitioned registration" \
  --type=task --priority=3 \
  --description="Investigate what happens when files in key=value/ directories are registered as non-partitioned Delta tables. Questions: (1) Does Spark still read data correctly? (2) Performance implications? (3) How to retroactively add partitioning? (4) Correctness when partition columns only exist in directory paths, not parquet files?"
```

**Step 2: Create DatabricksListing research bead**

```bash
bd create --title="Research: DatabricksListing implementation approach" \
  --type=task --priority=3 \
  --description="Investigate best approach for listing S3 files using Databricks auth instead of boto3. Candidates: dbutils.fs.ls(), Spark filesystem APIs, spark._jvm.org.apache.hadoop.fs. Evaluate parallelization, performance, and auth requirements."
```

**Step 3: Create parallelism research bead**

```bash
bd create --title="Research: parallelism strategy for convert() bulk operations" \
  --type=task --priority=4 \
  --description="Current approach uses ThreadPoolExecutor. Evaluate if Spark-native parallelism (spark.sparkContext.parallelize + mapPartitions) would be better for large-scale operations. Consider: thread safety, resource utilization, error handling."
```

**Step 4: Commit beads**

```bash
bd sync
```

---

### Task 9: Run full test suite and fix any issues

**Step 1: Run all unit tests**

Run: `cd databricks/hive_to_delta && python -m pytest tests/test_models.py tests/test_schema.py tests/test_delta_log.py tests/test_discovery.py tests/test_listing.py tests/test_converter.py -v`
Expected: ALL PASS

**Step 2: Fix any failures**

Address issues as they arise — likely candidates:
- Import path issues in delta_log.py re-exports
- Mock setup issues in converter tests
- Edge cases in partition value parsing

**Step 3: Commit any fixes**

```bash
git add -A
git commit -m "Fix test issues from composable pipeline refactor"
```

---

### Dependency Graph

```
Task 1 (TableInfo) ──┬── Task 2 (schema.py)
                     ├── Task 3 (discovery.py)
                     └── Task 4 (listing.py)
                              │
                     ┌────────┴────────┐
                     ▼                  ▼
            Task 5 (converter.py)  Task 7 (UCDiscovery)
                     │
                     ▼
            Task 6 (__init__.py)
                     │
                     ▼
            Task 8 (research beads)
                     │
                     ▼
            Task 9 (full test suite)
```

Tasks 2, 3, 4 can be done in parallel after Task 1.
Task 7 can be done in parallel with Task 5.
Task 8 can be done anytime.
