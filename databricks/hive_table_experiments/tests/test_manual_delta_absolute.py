"""Test Manual Delta Log with absolute paths for external partitions.

This tests whether Delta Lake can read files referenced by absolute S3 paths
in the _delta_log, which would enable no-copy migration for exotic partition layouts.
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime

import boto3
import pytest

# Import bucket constants from centralized module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from hive_table_experiments.scenarios import BUCKET_EAST_1A


def get_s3_client(region="us-east-1"):
    """Get S3 client."""
    return boto3.client("s3", region_name=region)


def get_glue_client(region="us-east-1"):
    """Get Glue client."""
    return boto3.client("glue", region_name=region)


def parse_s3_path(s3_path):
    """Parse s3://bucket/key into (bucket, key)."""
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    parts = s3_path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def scan_partition_files_from_glue(glue_database, table_name, region="us-east-1"):
    """
    Scan all partition locations (including external) for Parquet files.
    Returns files with ABSOLUTE S3 paths.
    """
    glue = get_glue_client(region)
    s3 = get_s3_client(region)

    # Get table info
    table = glue.get_table(DatabaseName=glue_database, Name=table_name)["Table"]

    # Get partitions
    partitions = []
    paginator = glue.get_paginator("get_partitions")
    for page in paginator.paginate(DatabaseName=glue_database, TableName=table_name):
        partitions.extend(page["Partitions"])

    # Get partition column names from table
    partition_keys = [pk["Name"] for pk in table.get("PartitionKeys", [])]

    files = []
    total_rows_estimate = 0

    for partition in partitions:
        location = partition["StorageDescriptor"]["Location"].rstrip("/")
        partition_values = dict(zip(partition_keys, partition["Values"]))

        bucket, prefix = parse_s3_path(location)

        # For cross-region, need to use appropriate client
        file_region = region
        if "west-2" in bucket:
            file_region = "us-west-2"

        s3_regional = boto3.client("s3", region_name=file_region)

        # List files in this partition
        try:
            paginator = s3_regional.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".parquet"):
                        # Use ABSOLUTE path
                        files.append({
                            "path": f"s3://{bucket}/{key}",
                            "size": obj["Size"],
                            "modificationTime": int(obj["LastModified"].timestamp() * 1000),
                            "partitionValues": partition_values,
                        })
                        # Estimate ~10 rows per file (based on our test data)
                        total_rows_estimate += 10
        except Exception as e:
            print(f"Warning: Could not scan {location}: {e}")

    return files, partition_keys, total_rows_estimate


def get_schema_from_glue(glue_database, table_name, region="us-east-1"):
    """Get Delta schema from Glue table."""
    glue = get_glue_client(region)
    table = glue.get_table(DatabaseName=glue_database, Name=table_name)["Table"]

    type_mapping = {
        "bigint": "long",
        "int": "integer",
        "string": "string",
        "double": "double",
        "float": "float",
        "boolean": "boolean",
    }

    fields = []
    # Data columns
    for col in table["StorageDescriptor"]["Columns"]:
        delta_type = type_mapping.get(col["Type"].lower(), col["Type"].lower())
        fields.append({
            "name": col["Name"],
            "type": delta_type,
            "nullable": True,
            "metadata": {},
        })

    # Partition columns
    for col in table.get("PartitionKeys", []):
        delta_type = type_mapping.get(col["Type"].lower(), col["Type"].lower())
        fields.append({
            "name": col["Name"],
            "type": delta_type,
            "nullable": True,
            "metadata": {},
        })

    return {"type": "struct", "fields": fields}


def generate_delta_log_json(files, schema, partition_columns):
    """Generate Delta log JSON with absolute paths."""
    table_id = str(uuid.uuid4())
    timestamp = int(datetime.now().timestamp() * 1000)

    entries = []

    # Protocol
    entries.append(json.dumps({
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2,
        }
    }))

    # Metadata
    entries.append(json.dumps({
        "metaData": {
            "id": table_id,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps(schema),
            "partitionColumns": partition_columns,
            "configuration": {},
            "createdTime": timestamp,
        }
    }))

    # Add files with ABSOLUTE paths
    for f in files:
        entries.append(json.dumps({
            "add": {
                "path": f["path"],  # This is the absolute S3 path
                "partitionValues": f["partitionValues"],
                "size": f["size"],
                "modificationTime": f["modificationTime"],
                "dataChange": True,
            }
        }))

    return "\n".join(entries)


def write_delta_log(delta_log_json, s3_location, region="us-east-1"):
    """Write Delta log to S3."""
    s3 = get_s3_client(region)
    bucket, prefix = parse_s3_path(s3_location.rstrip("/"))

    log_key = f"{prefix}/_delta_log/00000000000000000000.json"

    s3.put_object(
        Bucket=bucket,
        Key=log_key,
        Body=delta_log_json.encode("utf-8"),
        ContentType="application/json",
    )

    return f"s3://{bucket}/{log_key}"


def delete_delta_log(s3_location, region="us-east-1"):
    """Delete existing Delta log."""
    s3 = get_s3_client(region)
    bucket, prefix = parse_s3_path(s3_location.rstrip("/"))

    delta_log_prefix = f"{prefix}/_delta_log/"

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=delta_log_prefix):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
                print(f"  Deleted: {obj['Key']}")
    except Exception as e:
        print(f"Warning during cleanup: {e}")


# Test scenarios - table name to S3 base path mapping
MANUAL_SCENARIOS = {
    "scattered_src_manual": {
        "s3_base": f"s3://{BUCKET_EAST_1A}/conversion_eval/scattered/manual/",
        "expected_partitions": 2,  # us-east (under root), us-west (external)
        "expected_rows": 20,  # 10 rows per partition
    },
    "cross_bucket_src_manual": {
        "s3_base": f"s3://{BUCKET_EAST_1A}/conversion_eval/cross_bucket/manual/",
        "expected_partitions": 2,
        "expected_rows": 20,
    },
    "cross_region_src_manual": {
        "s3_base": f"s3://{BUCKET_EAST_1A}/conversion_eval/cross_region/manual/",
        "expected_partitions": 2,
        "expected_rows": 20,
    },
}


@pytest.fixture(scope="session")
def spark():
    """Create Databricks Connect session."""
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def uc_catalog():
    """UC catalog name."""
    return os.getenv("HIVE_EVAL_UC_CATALOG", "your_catalog")


@pytest.fixture(scope="session")
def uc_schema():
    """UC schema name."""
    return os.getenv("HIVE_EVAL_UC_SCHEMA", "your_uc_schema")


@pytest.fixture(scope="session")
def glue_database():
    """Glue database name."""
    return os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database")


class TestManualDeltaLogAbsolutePaths:
    """Test Manual Delta Log creation with absolute S3 paths."""

    @pytest.mark.parametrize("scenario", list(MANUAL_SCENARIOS.keys()))
    def test_manual_delta_with_absolute_paths(
        self,
        spark,
        scenario,
        glue_database,
        uc_catalog,
        uc_schema,
    ):
        """Test manual Delta log creation with absolute paths for external partitions."""

        scenario_config = MANUAL_SCENARIOS[scenario]
        s3_base_path = scenario_config["s3_base"]
        expected_rows = scenario_config["expected_rows"]

        print(f"\n{'='*60}")
        print(f"Testing Manual Delta Log (Absolute Paths): {scenario}")
        print(f"{'='*60}")

        base_scenario = scenario.replace("_src_manual", "")
        target_table = f"{uc_catalog}.{uc_schema}.{base_scenario}_delta_manual_absolute"

        print(f"Glue table: {glue_database}.{scenario}")
        print(f"S3 base path: {s3_base_path}")
        print(f"Target table: {target_table}")

        # Step 1: Get partition files with ABSOLUTE paths from Glue
        print("\n1. Scanning partition files with absolute paths from Glue...")
        files, partition_keys, source_estimate = scan_partition_files_from_glue(glue_database, scenario)
        print(f"   Found {len(files)} files across {len(set(tuple(f['partitionValues'].items()) for f in files))} partitions")
        for f in files:
            print(f"     - {f['path']}")
            print(f"       Partition: {f['partitionValues']}")

        # Step 2: Get schema
        print("\n2. Getting schema from Glue...")
        schema = get_schema_from_glue(glue_database, scenario)
        print(f"   Schema fields: {[f['name'] for f in schema['fields']]}")
        print(f"   Partition columns: {partition_keys}")

        # Step 3: Delete any existing Delta log
        print("\n3. Cleaning up existing Delta log...")
        delete_delta_log(s3_base_path)

        # Step 4: Generate and write Delta log with absolute paths
        print("\n4. Generating Delta log with absolute paths...")
        delta_log_json = generate_delta_log_json(files, schema, partition_keys)

        # Print the Delta log for inspection
        print("   Delta log content:")
        for line in delta_log_json.split("\n"):
            entry = json.loads(line)
            if "add" in entry:
                print(f"     ADD: {entry['add']['path']}")
            elif "metaData" in entry:
                print(f"     METADATA: id={entry['metaData']['id'][:8]}...")
            elif "protocol" in entry:
                print(f"     PROTOCOL: reader={entry['protocol']['minReaderVersion']}, writer={entry['protocol']['minWriterVersion']}")

        log_path = write_delta_log(delta_log_json, s3_base_path)
        print(f"   Wrote: {log_path}")

        # Step 5: Register in Unity Catalog
        print("\n5. Registering in Unity Catalog...")
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table}
            USING DELTA
            LOCATION '{s3_base_path}'
        """
        print(f"   SQL: {create_sql.strip()}")
        spark.sql(create_sql)

        # Step 6: Count target rows
        print("\n6. Counting target rows...")
        try:
            target_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]["cnt"]
            print(f"   Target row count: {target_count}")
        except Exception as e:
            error_str = str(e)
            print(f"   ERROR reading target: {error_str[:200]}...")

            # This is the key test - does Delta support absolute paths?
            if "FileNotFoundException" in error_str or "path does not exist" in error_str.lower():
                print("\n   ❌ ABSOLUTE PATHS NOT SUPPORTED - Delta cannot find files via absolute S3 paths")
                pytest.fail("Delta Lake does not support absolute S3 paths in the transaction log")
            else:
                pytest.fail(f"Unexpected error: {e}")

        # Step 7: Verify
        print("\n7. Verification:")
        print(f"   Expected rows: {expected_rows}")
        print(f"   Target rows: {target_count}")

        if target_count == expected_rows:
            print("   ✅ ROW COUNTS MATCH - Manual Delta Log with absolute paths WORKS!")
        else:
            print(f"   ⚠️ Row count difference: expected {expected_rows}, got {target_count}")

        # The key assertion - did we get ALL the data including external partitions?
        assert target_count == expected_rows, \
            f"Row count mismatch: expected={expected_rows}, got={target_count}"
