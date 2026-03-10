#!/usr/bin/env python3
"""
Focused Test: Can UC credentials resolve absolute S3 paths in Delta logs?

This test creates a minimal Delta table with absolute S3 paths in the
transaction log and tests if UC can vend credentials for those paths.

Hypothesis: UC credential resolution is path-based, and when Delta encounters
an absolute path like "s3://bucket/path/file.parquet" it cannot match it
to an external location (which maps s3://bucket/ -> credential).

Usage:
    source .venv/bin/activate
    AWS_PROFILE="aws-sandbox-field-eng_databricks-sandbox-admin" python tests/test_absolute_path_hypothesis.py
"""

import json
import os
import sys
from datetime import datetime
import uuid

import boto3
from databricks.connect import DatabricksSession

# Import bucket constants from centralized module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from hive_table_experiments.scenarios import BUCKET_EAST_1A

# =============================================================================
# Configuration
# =============================================================================

UC_CATALOG = os.getenv("HIVE_EVAL_UC_CATALOG", "your_catalog")
UC_SCHEMA = os.getenv("HIVE_EVAL_UC_SCHEMA", "your_uc_schema")
AWS_REGION = "us-east-1"

# Use existing parquet file paths
PARQUET_FILE_PATHS = [
    f"s3://{BUCKET_EAST_1A}/conversion_eval/scattered/manual/region=us-east/data.parquet",
    f"s3://{BUCKET_EAST_1A}/conversion_eval/scattered/manual/region=us-west/data.parquet",
]

# Where to put our test Delta log (separate from data files)
DELTA_TABLE_ROOT = f"s3://{BUCKET_EAST_1A}/tests/absolute_path_test/"

# UC table name
UC_TABLE_NAME = f"{UC_CATALOG}.{UC_SCHEMA}.absolute_path_hypothesis_test"


def get_s3_client(region="us-east-1"):
    return boto3.client("s3", region_name=region)


def parse_s3_path(s3_path: str) -> tuple:
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    parts = s3_path.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def get_file_info(s3_path: str) -> dict:
    """Get file size and modification time from S3."""
    s3 = get_s3_client()
    bucket, key = parse_s3_path(s3_path)

    response = s3.head_object(Bucket=bucket, Key=key)
    return {
        "path": s3_path,  # ABSOLUTE PATH
        "size": response["ContentLength"],
        "modificationTime": int(response["LastModified"].timestamp() * 1000),
    }


def create_delta_log_with_absolute_paths(file_paths: list) -> str:
    """Create a Delta log JSON with absolute S3 paths."""

    table_id = str(uuid.uuid4())
    timestamp = int(datetime.now().timestamp() * 1000)

    # Infer schema from file naming (simple test schema)
    schema = {
        "type": "struct",
        "fields": [
            {"name": "id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "value", "type": "double", "nullable": True, "metadata": {}},
            {"name": "region", "type": "string", "nullable": True, "metadata": {}},
        ]
    }

    entries = []

    # Protocol
    entries.append(json.dumps({
        "protocol": {"minReaderVersion": 1, "minWriterVersion": 2}
    }))

    # Metadata
    entries.append(json.dumps({
        "metaData": {
            "id": table_id,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps(schema),
            "partitionColumns": ["region"],
            "configuration": {},
            "createdTime": timestamp,
        }
    }))

    # Add files with ABSOLUTE paths
    for file_path in file_paths:
        file_info = get_file_info(file_path)

        # Extract partition value from path
        region = "us-east" if "region=us-east" in file_path else "us-west"

        entries.append(json.dumps({
            "add": {
                "path": file_info["path"],  # ABSOLUTE S3 PATH
                "partitionValues": {"region": region},
                "size": file_info["size"],
                "modificationTime": file_info["modificationTime"],
                "dataChange": True,
            }
        }))

    return "\n".join(entries)


def write_delta_log(delta_log_json: str, table_root: str) -> str:
    """Write Delta log to S3."""
    s3 = get_s3_client()
    bucket, prefix = parse_s3_path(table_root.rstrip("/"))

    log_key = f"{prefix}/_delta_log/00000000000000000000.json"

    s3.put_object(
        Bucket=bucket,
        Key=log_key,
        Body=delta_log_json.encode("utf-8"),
        ContentType="application/json",
    )

    return f"s3://{bucket}/{log_key}"


def cleanup_delta_log(table_root: str):
    """Remove existing Delta log."""
    s3 = get_s3_client()
    bucket, prefix = parse_s3_path(table_root.rstrip("/"))

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/_delta_log/"):
            for obj in page.get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
                print(f"  Deleted: {obj['Key']}")
    except Exception as e:
        print(f"  Cleanup warning: {e}")


def main():
    print("=" * 70)
    print("HYPOTHESIS TEST: Can UC resolve credentials for absolute S3 paths?")
    print("=" * 70)
    print(f"Started: {datetime.now().isoformat()}")

    # Connect to Databricks
    print("\n1. Connecting to Databricks...")
    spark = DatabricksSession.builder.getOrCreate()
    print("   Connected!")

    # Clean up any existing test artifacts
    print("\n2. Cleaning up existing test artifacts...")
    cleanup_delta_log(DELTA_TABLE_ROOT)
    try:
        spark.sql(f"DROP TABLE IF EXISTS {UC_TABLE_NAME}")
        print(f"   Dropped existing table: {UC_TABLE_NAME}")
    except Exception as e:
        print(f"   Table cleanup note: {e}")

    # Verify source files exist
    print("\n3. Verifying source parquet files exist...")
    s3 = get_s3_client()
    for path in PARQUET_FILE_PATHS:
        bucket, key = parse_s3_path(path)
        try:
            s3.head_object(Bucket=bucket, Key=key)
            print(f"   Found: {path}")
        except Exception as e:
            print(f"   MISSING: {path} - {e}")
            sys.exit(1)

    # Create Delta log with absolute paths
    print("\n4. Creating Delta log with ABSOLUTE S3 paths...")
    delta_log_json = create_delta_log_with_absolute_paths(PARQUET_FILE_PATHS)

    # Print the delta log for inspection
    print("   Delta log content:")
    for line in delta_log_json.split("\n"):
        entry = json.loads(line)
        if "add" in entry:
            print(f"     ADD: {entry['add']['path']}")
        elif "metaData" in entry:
            print(f"     METADATA: id={entry['metaData']['id'][:8]}...")
        elif "protocol" in entry:
            print(f"     PROTOCOL: reader={entry['protocol']['minReaderVersion']}")

    log_path = write_delta_log(delta_log_json, DELTA_TABLE_ROOT)
    print(f"   Wrote to: {log_path}")

    # Register table in UC
    print("\n5. Registering table in Unity Catalog...")
    create_sql = f"""
        CREATE TABLE {UC_TABLE_NAME}
        USING DELTA
        LOCATION '{DELTA_TABLE_ROOT}'
    """
    print(f"   SQL: {create_sql.strip()}")
    spark.sql(create_sql)
    print("   Table created successfully!")

    # TEST 1: Try to count rows
    print("\n6. TEST 1: SELECT COUNT(*)")
    try:
        result = spark.sql(f"SELECT COUNT(*) as cnt FROM {UC_TABLE_NAME}").collect()
        count = result[0]["cnt"]
        print(f"   RESULT: {count} rows")
        print("   SUCCESS - UC resolved credentials for absolute paths!")
    except Exception as e:
        error_str = str(e)
        print(f"   ERROR: {error_str[:200]}...")

        if "FileNotFoundException" in error_str:
            print("\n   ANALYSIS: FileNotFoundException indicates Delta tried to read")
            print("   the file but couldn't find it at the absolute path.")
            print("   This suggests UC is NOT vending credentials for absolute paths.")

        elif "AccessDeniedException" in error_str or "Access Denied" in error_str:
            print("\n   ANALYSIS: Access Denied indicates Delta found the path but")
            print("   couldn't authenticate. UC may not be matching absolute paths")
            print("   to external locations.")

        elif "credentials" in error_str.lower():
            print("\n   ANALYSIS: Credential error - UC cannot vend credentials")
            print("   for absolute S3 paths in Delta transaction log.")

    # TEST 2: Try delta.`path` syntax
    print("\n7. TEST 2: delta.`path` syntax (direct path access)")
    try:
        result = spark.sql(f"SELECT COUNT(*) as cnt FROM delta.`{DELTA_TABLE_ROOT}`").collect()
        count = result[0]["cnt"]
        print(f"   RESULT: {count} rows")
        print("   SUCCESS - Direct delta path access works!")
    except Exception as e:
        print(f"   ERROR: {str(e)[:200]}...")

    # TEST 3: Try to read source parquet directly
    print("\n8. TEST 3: Direct parquet read (bypass Delta)")
    test_path = PARQUET_FILE_PATHS[0]
    print(f"   Testing path: {test_path}")
    try:
        df = spark.read.parquet(test_path)
        count = df.count()
        print(f"   RESULT: {count} rows")
        print("   SUCCESS - UC credentials work for direct parquet access!")
    except Exception as e:
        print(f"   ERROR: {str(e)[:200]}...")
        print("   This suggests UC credentials may not be available at all")

    # TEST 4: Read parquet via SQL path syntax
    print("\n9. TEST 4: parquet.`path` SQL syntax")
    test_dir = f"s3://{BUCKET_EAST_1A}/conversion_eval/scattered/manual/region=us-east/"
    print(f"   Testing path: {test_dir}")
    try:
        result = spark.sql(f"SELECT COUNT(*) as cnt FROM parquet.`{test_dir}`").collect()
        count = result[0]["cnt"]
        print(f"   RESULT: {count} rows")
        print("   SUCCESS - parquet.`path` works!")
    except Exception as e:
        print(f"   ERROR: {str(e)[:200]}...")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print("""
The hypothesis test results indicate whether UC can vend credentials for
absolute S3 paths stored in Delta transaction logs.

If TEST 1 fails but TEST 3/4 succeed:
  -> UC can vend credentials for path-based access
  -> Delta CANNOT use credentials for absolute paths in _delta_log
  -> This confirms the architectural limitation

If all tests fail:
  -> There may be a configuration issue with UC credentials
  -> Check external location setup and permissions

If all tests succeed:
  -> Absolute paths in Delta logs WORK (contrary to hypothesis)
  -> Manual Delta Log approach is viable
""")

    # Cleanup
    print("\n10. Cleaning up test table...")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {UC_TABLE_NAME}")
        print(f"   Dropped: {UC_TABLE_NAME}")
    except Exception as e:
        print(f"   Cleanup note: {e}")

    print(f"\nCompleted: {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
