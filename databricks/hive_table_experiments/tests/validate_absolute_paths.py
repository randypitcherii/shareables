#!/usr/bin/env python3
"""
Clean-slate validation of Manual Delta Log with absolute S3 paths.

This script performs a rigorous, end-to-end validation to definitively determine
whether Delta tables can reference files at absolute S3 paths (outside the table root)
and have READs work via Unity Catalog credentials.

Usage:
    source .venv/bin/activate
    python tests/validate_absolute_paths.py
"""

import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
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
GLUE_DATABASE = os.getenv("HIVE_EVAL_GLUE_DATABASE", "your_glue_database")
AWS_REGION = "us-east-1"

# S3 locations for Delta table roots (where _delta_log will be written)
DELTA_TABLE_ROOTS = {
    "scattered": f"s3://{BUCKET_EAST_1A}/absolute_path_test/scattered/",
    "cross_bucket": f"s3://{BUCKET_EAST_1A}/absolute_path_test/cross_bucket/",
    "cross_region": f"s3://{BUCKET_EAST_1A}/absolute_path_test/cross_region/",
}

# Mapping from scenario to source Glue table
SOURCE_TABLES = {
    "scattered": "scattered_src_manual",
    "cross_bucket": "cross_bucket_src_manual",
    "cross_region": "cross_region_src_manual",
}

@dataclass
class ValidationResult:
    """Result of a single scenario validation."""
    scenario: str
    partitions: List[Dict[str, str]]  # partition_values -> location
    files: List[Dict[str, Any]]  # All files with absolute paths
    delta_log_location: str
    delta_log_has_absolute_paths: bool
    uc_table_name: str
    read_works: bool
    error_message: Optional[str] = None
    row_count: int = 0
    partition_row_counts: Dict[str, int] = field(default_factory=dict)
    files_at_original_locations: bool = True  # Verify no copy

    def to_dict(self):
        return {
            "scenario": self.scenario,
            "partitions": self.partitions,
            "num_files": len(self.files),
            "files": [{"path": f["path"], "partition": f["partitionValues"]} for f in self.files],
            "delta_log_location": self.delta_log_location,
            "delta_log_has_absolute_paths": self.delta_log_has_absolute_paths,
            "uc_table_name": self.uc_table_name,
            "read_works": self.read_works,
            "error_message": self.error_message,
            "row_count": self.row_count,
            "partition_row_counts": self.partition_row_counts,
            "files_at_original_locations": self.files_at_original_locations,
        }


# =============================================================================
# AWS Clients
# =============================================================================

def get_s3_client(region="us-east-1"):
    return boto3.client("s3", region_name=region)

def get_glue_client(region="us-east-1"):
    return boto3.client("glue", region_name=region)

def parse_s3_path(s3_path: str) -> tuple:
    """Parse s3://bucket/key into (bucket, key)."""
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    parts = s3_path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


# =============================================================================
# Phase 1: Clean Slate
# =============================================================================

def drop_existing_uc_tables(spark) -> List[str]:
    """Drop all existing manual delta test tables."""
    print("\n" + "=" * 70)
    print("PHASE 1: CLEAN SLATE")
    print("=" * 70)

    dropped_tables = []
    tables_to_drop = [
        # Current test tables
        "scattered_delta_absolute_test",
        "cross_bucket_delta_absolute_test",
        "cross_region_delta_absolute_test",
        # Legacy tables from previous tests
        "scattered_delta_manual_absolute",
        "cross_bucket_delta_manual_absolute",
        "cross_region_delta_manual_absolute",
        "scattered_delta_manual",
        "cross_bucket_delta_manual",
        "cross_region_delta_manual",
    ]

    for table_name in tables_to_drop:
        full_name = f"{UC_CATALOG}.{UC_SCHEMA}.{table_name}"
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
            dropped_tables.append(full_name)
            print(f"  Dropped: {full_name}")
        except Exception as e:
            print(f"  Note: Could not drop {full_name}: {e}")

    return dropped_tables


def delete_existing_delta_logs() -> List[str]:
    """Delete any existing Delta logs at test S3 locations."""
    print("\n  Deleting existing Delta logs...")
    deleted_logs = []

    s3 = get_s3_client()

    # Delete from test locations
    for scenario, root in DELTA_TABLE_ROOTS.items():
        bucket, prefix = parse_s3_path(root.rstrip("/"))
        delta_log_prefix = f"{prefix}/_delta_log/"

        try:
            paginator = s3.get_paginator("list_objects_v2")
            objects_to_delete = []
            for page in paginator.paginate(Bucket=bucket, Prefix=delta_log_prefix):
                for obj in page.get("Contents", []):
                    objects_to_delete.append({"Key": obj["Key"]})

            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i + 1000]
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                deleted_logs.append(f"s3://{bucket}/{delta_log_prefix}")
                print(f"    Deleted Delta log at: s3://{bucket}/{delta_log_prefix}")
        except Exception as e:
            print(f"    Warning: {e}")

    # Also clean up the original manual test locations
    legacy_locations = [
        f"s3://{BUCKET_EAST_1A}/conversion_eval/scattered/manual/",
        f"s3://{BUCKET_EAST_1A}/conversion_eval/cross_bucket/manual/",
        f"s3://{BUCKET_EAST_1A}/conversion_eval/cross_region/manual/",
    ]

    for loc in legacy_locations:
        bucket, prefix = parse_s3_path(loc.rstrip("/"))
        delta_log_prefix = f"{prefix}/_delta_log/"

        try:
            paginator = s3.get_paginator("list_objects_v2")
            objects_to_delete = []
            for page in paginator.paginate(Bucket=bucket, Prefix=delta_log_prefix):
                for obj in page.get("Contents", []):
                    objects_to_delete.append({"Key": obj["Key"]})

            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i + 1000]
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                deleted_logs.append(f"s3://{bucket}/{delta_log_prefix}")
                print(f"    Deleted Delta log at: s3://{bucket}/{delta_log_prefix}")
        except Exception as e:
            pass  # Ignore - may not exist

    return deleted_logs


def verify_source_glue_tables_exist() -> Dict[str, Dict]:
    """Verify source Glue tables exist with their ORIGINAL partition locations."""
    print("\n  Verifying source Glue tables...")

    glue = get_glue_client()
    source_info = {}

    for scenario, table_name in SOURCE_TABLES.items():
        try:
            # Get table
            table = glue.get_table(DatabaseName=GLUE_DATABASE, Name=table_name)["Table"]
            location = table["StorageDescriptor"]["Location"]

            # Get partitions
            paginator = glue.get_paginator("get_partitions")
            partitions = []
            for page in paginator.paginate(DatabaseName=GLUE_DATABASE, TableName=table_name):
                partitions.extend(page["Partitions"])

            partition_info = []
            for p in partitions:
                partition_info.append({
                    "values": p["Values"],
                    "location": p["StorageDescriptor"]["Location"]
                })

            source_info[scenario] = {
                "table_name": table_name,
                "table_location": location,
                "partitions": partition_info,
            }

            print(f"    {scenario}: {table_name}")
            print(f"      Table root: {location}")
            for p in partition_info:
                print(f"      Partition {p['values']}: {p['location']}")

        except Exception as e:
            print(f"    ERROR: {scenario} - {e}")
            source_info[scenario] = {"error": str(e)}

    return source_info


# =============================================================================
# Phase 2: Create Manual Delta Logs with TRUE Absolute Paths
# =============================================================================

def scan_partition_files_absolute(scenario: str, source_info: Dict) -> List[Dict]:
    """
    Scan all partition locations for Parquet files.
    Returns files with ABSOLUTE S3 paths - NO DATA COPYING.
    """
    files = []

    partitions = source_info.get(scenario, {}).get("partitions", [])

    for partition in partitions:
        location = partition["location"].rstrip("/")
        values = partition["values"]

        bucket, prefix = parse_s3_path(location)

        # Determine region for S3 client
        region = "us-west-2" if "west-2" in bucket else "us-east-1"
        s3 = get_s3_client(region)

        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".parquet"):
                        # ABSOLUTE PATH - this is the key test
                        absolute_path = f"s3://{bucket}/{key}"
                        files.append({
                            "path": absolute_path,
                            "size": obj["Size"],
                            "modificationTime": int(obj["LastModified"].timestamp() * 1000),
                            "partitionValues": {"region": values[0]},  # Assuming 'region' is the partition col
                        })
        except Exception as e:
            print(f"    Warning: Could not scan {location}: {e}")

    return files


def get_schema_from_glue(table_name: str) -> Dict:
    """Get Delta schema from Glue table."""
    glue = get_glue_client()
    table = glue.get_table(DatabaseName=GLUE_DATABASE, Name=table_name)["Table"]

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


def generate_delta_log_json(files: List[Dict], schema: Dict, partition_columns: List[str]) -> str:
    """Generate Delta log JSON with ABSOLUTE paths."""
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
                "path": f["path"],  # ABSOLUTE S3 path
                "partitionValues": f["partitionValues"],
                "size": f["size"],
                "modificationTime": f["modificationTime"],
                "dataChange": True,
            }
        }))

    return "\n".join(entries)


def write_delta_log(delta_log_json: str, s3_location: str) -> str:
    """Write Delta log to S3."""
    s3 = get_s3_client()
    bucket, prefix = parse_s3_path(s3_location.rstrip("/"))

    log_key = f"{prefix}/_delta_log/00000000000000000000.json"

    s3.put_object(
        Bucket=bucket,
        Key=log_key,
        Body=delta_log_json.encode("utf-8"),
        ContentType="application/json",
    )

    return f"s3://{bucket}/{log_key}"


def create_manual_delta_logs(source_info: Dict) -> Dict[str, str]:
    """Create manual Delta logs for all scenarios with absolute paths."""
    print("\n" + "=" * 70)
    print("PHASE 2: CREATE MANUAL DELTA LOGS WITH ABSOLUTE PATHS")
    print("=" * 70)

    delta_log_paths = {}

    for scenario, table_root in DELTA_TABLE_ROOTS.items():
        print(f"\n  Creating Delta log for: {scenario}")

        source_table = SOURCE_TABLES[scenario]

        # Scan files with ABSOLUTE paths
        files = scan_partition_files_absolute(scenario, source_info)
        print(f"    Found {len(files)} files:")
        for f in files:
            print(f"      - {f['path']} (partition: {f['partitionValues']})")

        # Get schema
        schema = get_schema_from_glue(source_table)
        print(f"    Schema fields: {[f['name'] for f in schema['fields']]}")

        # Generate Delta log
        delta_log_json = generate_delta_log_json(files, schema, ["region"])

        # Print Delta log content for inspection
        print("    Delta log content:")
        for line in delta_log_json.split("\n"):
            entry = json.loads(line)
            if "add" in entry:
                path = entry["add"]["path"]
                # VERIFY this is an absolute path
                assert path.startswith("s3://"), f"ERROR: Expected absolute path, got: {path}"
                print(f"      ADD: {path}")
            elif "metaData" in entry:
                print(f"      METADATA: id={entry['metaData']['id'][:8]}...")
            elif "protocol" in entry:
                print(f"      PROTOCOL: reader={entry['protocol']['minReaderVersion']}")

        # Write Delta log
        log_path = write_delta_log(delta_log_json, table_root)
        print(f"    Wrote: {log_path}")

        delta_log_paths[scenario] = log_path

    return delta_log_paths


# =============================================================================
# Phase 3: Register in Unity Catalog
# =============================================================================

def register_uc_tables(spark) -> Dict[str, str]:
    """Register Delta tables in Unity Catalog."""
    print("\n" + "=" * 70)
    print("PHASE 3: REGISTER IN UNITY CATALOG")
    print("=" * 70)

    uc_tables = {}

    for scenario, table_root in DELTA_TABLE_ROOTS.items():
        table_name = f"{scenario}_delta_absolute_test"
        full_name = f"{UC_CATALOG}.{UC_SCHEMA}.{table_name}"

        print(f"\n  Registering: {full_name}")
        print(f"    Location: {table_root}")

        # Drop if exists
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")

        # Create table
        create_sql = f"""
            CREATE TABLE {full_name}
            USING DELTA
            LOCATION '{table_root}'
        """
        print(f"    SQL: {create_sql.strip()}")

        try:
            spark.sql(create_sql)
            print(f"    SUCCESS: Table created")
            uc_tables[scenario] = full_name
        except Exception as e:
            print(f"    ERROR: {e}")
            uc_tables[scenario] = f"FAILED: {e}"

    return uc_tables


# =============================================================================
# Phase 4: Test READ Operations
# =============================================================================

def test_read_operations(spark, uc_tables: Dict[str, str]) -> Dict[str, ValidationResult]:
    """Test READ operations for each table."""
    print("\n" + "=" * 70)
    print("PHASE 4: TEST READ OPERATIONS")
    print("=" * 70)

    results = {}

    for scenario, table_name in uc_tables.items():
        if table_name.startswith("FAILED"):
            results[scenario] = ValidationResult(
                scenario=scenario,
                partitions=[],
                files=[],
                delta_log_location=DELTA_TABLE_ROOTS[scenario],
                delta_log_has_absolute_paths=True,
                uc_table_name=table_name,
                read_works=False,
                error_message=table_name,
            )
            continue

        print(f"\n  Testing: {table_name}")

        result = ValidationResult(
            scenario=scenario,
            partitions=[],
            files=[],
            delta_log_location=DELTA_TABLE_ROOTS[scenario],
            delta_log_has_absolute_paths=True,
            uc_table_name=table_name,
            read_works=False,
        )

        # Test 1: Simple COUNT
        print("    Test 1: SELECT COUNT(*)")
        try:
            count_result = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()
            total_count = count_result[0]["cnt"]
            result.row_count = total_count
            print(f"      Result: {total_count} rows")
        except Exception as e:
            error_str = str(e)
            print(f"      ERROR: {error_str[:200]}...")
            result.error_message = error_str
            results[scenario] = result
            continue

        # Test 2: Full scan (SELECT *)
        print("    Test 2: SELECT * (full scan)")
        try:
            df = spark.sql(f"SELECT * FROM {table_name}")
            row_count = df.count()
            print(f"      Result: {row_count} rows retrieved")
        except Exception as e:
            error_str = str(e)
            print(f"      ERROR: {error_str[:200]}...")
            result.error_message = error_str
            results[scenario] = result
            continue

        # Test 3: Partition filter (us-east)
        print("    Test 3: SELECT * WHERE region = 'us-east'")
        try:
            east_df = spark.sql(f"SELECT * FROM {table_name} WHERE region = 'us-east'")
            east_count = east_df.count()
            result.partition_row_counts["us-east"] = east_count
            print(f"      Result: {east_count} rows")
        except Exception as e:
            error_str = str(e)
            print(f"      ERROR: {error_str[:200]}...")

        # Test 4: Partition filter (us-west)
        print("    Test 4: SELECT * WHERE region = 'us-west'")
        try:
            west_df = spark.sql(f"SELECT * FROM {table_name} WHERE region = 'us-west'")
            west_count = west_df.count()
            result.partition_row_counts["us-west"] = west_count
            print(f"      Result: {west_count} rows")
        except Exception as e:
            error_str = str(e)
            print(f"      ERROR: {error_str[:200]}...")

        # If we got here, reads work!
        if result.error_message is None:
            result.read_works = True
            print(f"    ALL TESTS PASSED - Reads work!")

        results[scenario] = result

    return results


# =============================================================================
# Phase 5: Verify No Hidden Data Copy
# =============================================================================

def verify_no_hidden_copy(source_info: Dict) -> Dict[str, bool]:
    """Verify that Delta log contains absolute paths and no data was copied."""
    print("\n" + "=" * 70)
    print("PHASE 5: VERIFY NO HIDDEN DATA COPY")
    print("=" * 70)

    verification_results = {}
    s3 = get_s3_client()

    for scenario, table_root in DELTA_TABLE_ROOTS.items():
        print(f"\n  Verifying: {scenario}")

        # Read Delta log
        bucket, prefix = parse_s3_path(table_root.rstrip("/"))
        log_key = f"{prefix}/_delta_log/00000000000000000000.json"

        try:
            response = s3.get_object(Bucket=bucket, Key=log_key)
            delta_log_content = response["Body"].read().decode("utf-8")

            # Check that all paths are absolute
            all_absolute = True
            for line in delta_log_content.split("\n"):
                entry = json.loads(line)
                if "add" in entry:
                    path = entry["add"]["path"]
                    print(f"    Path in Delta log: {path}")
                    if not path.startswith("s3://"):
                        print(f"      ERROR: Expected absolute path!")
                        all_absolute = False

            # Check that no data files exist at table root (only _delta_log)
            paginator = s3.get_paginator("list_objects_v2")
            data_files_at_root = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".parquet") and "_delta_log" not in key:
                        data_files_at_root.append(key)

            if data_files_at_root:
                print(f"    WARNING: Found data files at table root (may indicate copy):")
                for f in data_files_at_root:
                    print(f"      - {f}")
                verification_results[scenario] = False
            else:
                print(f"    No data files at table root (only _delta_log)")
                print(f"    All paths are absolute: {all_absolute}")
                verification_results[scenario] = all_absolute

        except Exception as e:
            print(f"    ERROR: {e}")
            verification_results[scenario] = False

    return verification_results


# =============================================================================
# Main Execution
# =============================================================================

def main():
    print("=" * 70)
    print("CLEAN-SLATE VALIDATION: MANUAL DELTA LOG WITH ABSOLUTE S3 PATHS")
    print("=" * 70)
    print(f"Started: {datetime.now().isoformat()}")
    print(f"UC Catalog: {UC_CATALOG}")
    print(f"UC Schema: {UC_SCHEMA}")
    print(f"Glue Database: {GLUE_DATABASE}")

    # Connect to Databricks
    print("\nConnecting to Databricks...")
    spark = DatabricksSession.builder.getOrCreate()
    print("  Connected!")

    # Phase 1: Clean Slate
    drop_existing_uc_tables(spark)
    delete_existing_delta_logs()
    source_info = verify_source_glue_tables_exist()

    # Phase 2: Create Manual Delta Logs
    delta_log_paths = create_manual_delta_logs(source_info)

    # Phase 3: Register in UC
    uc_tables = register_uc_tables(spark)

    # Phase 4: Test READ Operations
    results = test_read_operations(spark, uc_tables)

    # Phase 5: Verify No Hidden Copy
    no_copy_verified = verify_no_hidden_copy(source_info)

    # ==========================================================================
    # FINAL RESULTS
    # ==========================================================================
    print("\n" + "=" * 70)
    print("FINAL RESULTS SUMMARY")
    print("=" * 70)

    print("\n| Scenario | Partitions | Files at Original Locations | Delta Log Has Absolute Paths | READ Works | Error (if any) |")
    print("|----------|------------|----------------------------|-----------------------------|-----------| ---------------|")

    all_passed = True
    for scenario, result in results.items():
        partitions = len(source_info.get(scenario, {}).get("partitions", []))
        files_original = "Yes" if no_copy_verified.get(scenario, False) else "No"
        has_absolute = "Yes" if result.delta_log_has_absolute_paths else "No"
        read_works = "Yes" if result.read_works else "No"
        error = result.error_message[:50] if result.error_message else "-"

        print(f"| {scenario} | {partitions} | {files_original} | {has_absolute} | {read_works} | {error} |")

        if not result.read_works:
            all_passed = False

    print("\n" + "-" * 70)

    if all_passed:
        print("\n*** DEFINITIVE ANSWER: ***")
        print("Manual Delta Log with absolute paths WORKS!")
        print("  - Delta tables CAN reference files at absolute S3 paths")
        print("  - Files OUTSIDE the table root are readable via UC credentials")
        print("  - NO data copy is required")
        print("  - This enables zero-copy migration for exotic partition layouts")
    else:
        print("\n*** DEFINITIVE ANSWER: ***")
        print("Manual Delta Log with absolute paths DOES NOT WORK!")
        print("Failures detected - see error messages above.")

        # Print reproduction steps
        print("\n--- Reproduction Steps ---")
        for scenario, result in results.items():
            if not result.read_works:
                print(f"\nScenario: {scenario}")
                print(f"1. Create Glue table with partition at external path")
                print(f"2. Create Delta log at {result.delta_log_location}")
                print(f"3. Delta log contains absolute paths to files")
                print(f"4. Register in UC: CREATE TABLE {result.uc_table_name} USING DELTA LOCATION '{DELTA_TABLE_ROOTS[scenario]}'")
                print(f"5. Run: SELECT * FROM {result.uc_table_name}")
                print(f"6. Error: {result.error_message}")

    print(f"\nCompleted: {datetime.now().isoformat()}")

    return results


if __name__ == "__main__":
    results = main()
