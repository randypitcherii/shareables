#!/usr/bin/env python3
"""Verify test tables and data for hive_to_delta integration tests.

Queries AWS Glue and S3 to verify all test tables and data are correctly set up.
"""

import io

import boto3
import pyarrow.parquet as pq


# Configuration
AWS_PROFILE = "aws-sandbox-field-eng_databricks-sandbox-admin"
DEFAULT_REGION = "us-east-1"
DATABASE_NAME = "hive_to_delta_test"
TEST_TABLES = [
    "test_table_standard",
    "test_table_cross_bucket",
    "test_table_cross_region",
]


def get_boto3_session(region: str = DEFAULT_REGION):
    """Get boto3 session with explicit AWS profile."""
    return boto3.Session(profile_name=AWS_PROFILE, region_name=region)


def verify_glue_table(table_name: str) -> dict:
    """Verify Glue table exists and get its details."""
    session = get_boto3_session()
    glue_client = session.client("glue")

    try:
        response = glue_client.get_table(DatabaseName=DATABASE_NAME, Name=table_name)
        table = response["Table"]

        return {
            "exists": True,
            "location": table["StorageDescriptor"]["Location"],
            "columns": [col["Name"] for col in table["StorageDescriptor"]["Columns"]],
            "partition_keys": [pk["Name"] for pk in table.get("PartitionKeys", [])],
        }
    except glue_client.exceptions.EntityNotFoundException:
        return {"exists": False}


def get_table_partitions(table_name: str) -> list:
    """Get all partitions for a table."""
    session = get_boto3_session()
    glue_client = session.client("glue")

    try:
        response = glue_client.get_partitions(DatabaseName=DATABASE_NAME, TableName=table_name)
        partitions = []
        for partition in response["Partitions"]:
            partitions.append(
                {
                    "values": partition["Values"],
                    "location": partition["StorageDescriptor"]["Location"],
                }
            )
        return partitions
    except glue_client.exceptions.EntityNotFoundException:
        return []


def read_parquet_from_s3(s3_path: str) -> dict:
    """Read parquet file from S3 and return summary."""
    # Parse S3 path
    parts = s3_path.replace("s3://", "").split("/")
    bucket = parts[0]
    key = "/".join(parts[1:])

    if not key.endswith(".parquet"):
        key = key.rstrip("/") + "/data.parquet"

    # Determine region from bucket name
    if "west-2" in bucket:
        region = "us-west-2"
    else:
        region = "us-east-1"

    try:
        session = get_boto3_session(region=region)
        s3 = session.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)
        buffer = io.BytesIO(response["Body"].read())
        table = pq.read_table(buffer)

        df = table.to_pandas()
        return {
            "exists": True,
            "row_count": len(df),
            "sample_names": df["name"].tolist()[:3] if "name" in df.columns else [],
        }
    except Exception as e:
        return {"exists": False, "error": str(e)}


def verify_all_tables() -> None:
    """Verify all test tables."""
    print("=" * 70)
    print("VERIFICATION: Integration Test Tables")
    print("=" * 70)
    print(f"AWS Profile: {AWS_PROFILE}")
    print(f"AWS Region: {DEFAULT_REGION}")
    print(f"Glue Database: {DATABASE_NAME}")
    print("=" * 70)

    for table_name in TEST_TABLES:
        print(f"\n{'='*70}")
        print(f"Table: {table_name}")
        print(f"{'='*70}")

        # Check table exists
        table_info = verify_glue_table(table_name)
        if not table_info["exists"]:
            print("  ERROR: Table does not exist!")
            continue

        print(f"  Location: {table_info['location']}")
        print(f"  Columns: {', '.join(table_info['columns'])}")
        print(f"  Partition Keys: {', '.join(table_info['partition_keys'])}")

        # Check partitions
        partitions = get_table_partitions(table_name)
        print(f"  Total Partitions: {len(partitions)}")

        for partition in partitions:
            print(f"\n  Partition: {partition['values']}")
            print(f"    Location: {partition['location']}")

            # Verify data file exists
            data_info = read_parquet_from_s3(partition["location"])
            if data_info["exists"]:
                print(f"    Status: OK")
                print(f"    Row Count: {data_info['row_count']}")
                print(f"    Sample Names: {', '.join(data_info['sample_names'])}")
            else:
                print(f"    Status: ERROR - {data_info.get('error', 'Unknown error')}")

    print(f"\n{'='*70}")
    print("Verification Complete")
    print(f"{'='*70}")


def main() -> None:
    """Main verification routine."""
    verify_all_tables()


if __name__ == "__main__":
    main()
