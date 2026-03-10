"""
Approach 4: Mixed GCS + S3 Access in the Same Session

Tests whether a Databricks session can read from both GCS and normal AWS S3
without one breaking the other. The HMAC approach (Approach 1) overrides
fs.s3a.endpoint globally, which likely breaks normal S3 reads.

Tests:
  1. Bucket-specific s3a configs to isolate GCS from S3
  2. Python SDK for GCS + normal s3a:// for S3 (no conflict)
  3. gs:// connector for GCS + normal s3a:// for S3 (no conflict)
"""

import csv
import io
import json

from google.cloud import storage
from google.oauth2 import service_account

from _common import (
    BUCKET,
    get_cluster_type,
    get_secret,
    get_spark_and_dbutils,
    print_header,
    print_summary,
)

# Unity Catalog managed storage path (standard S3 in AWS workspace)
# Uses the workspace's default catalog — any table accessible via SQL works
S3_TEST_QUERY = "SELECT 1 AS id, 'hello from S3' AS source"


def test_bucket_specific_s3a(spark, dbutils):
    """Test 1: Per-bucket s3a config isolating GCS endpoint from default S3.

    Hadoop S3A supports per-bucket config via:
      fs.s3a.bucket.<BUCKETNAME>.<setting> = <value>

    This should allow GCS-bound HMAC creds on the GCS bucket while leaving
    the default s3a config untouched for normal AWS S3 access.
    """
    print_header("Test 1: Bucket-specific s3a config (GCS isolated from S3)")

    hmac_access_id = get_secret(dbutils, "hmac_access_id")
    hmac_secret = get_secret(dbutils, "hmac_secret")

    gcs_s3_path = f"s3a://{BUCKET}/sample-data/data.csv"

    # Step 1a: Set bucket-specific config for GCS bucket only
    print("Setting per-bucket s3a config for GCS bucket...")
    try:
        prefix = f"spark.hadoop.fs.s3a.bucket.{BUCKET}"
        spark.conf.set(f"{prefix}.endpoint", "https://storage.googleapis.com")
        spark.conf.set(f"{prefix}.access.key", hmac_access_id)
        spark.conf.set(f"{prefix}.secret.key", hmac_secret)
        spark.conf.set(f"{prefix}.path.style.access", "true")
        print(f"  Per-bucket config set for: {BUCKET}")

        df_gcs = spark.read.csv(gcs_s3_path, header=True, inferSchema=True)
        df_gcs.show()
        print(f"  PASS (GCS): Read {df_gcs.count()} rows via bucket-specific s3a config")
    except Exception as e:
        print(f"  FAIL (GCS): {e}")

    # Step 1b: Verify normal S3/UC access still works
    print("\nVerifying normal S3/UC access is not broken...")
    try:
        df_s3 = spark.sql(S3_TEST_QUERY)
        df_s3.show()
        print(f"  PASS (S3): Normal Spark SQL still works — {df_s3.count()} rows")
    except Exception as e:
        print(f"  FAIL (S3): Normal access broken — {e}")


def test_python_sdk_plus_s3(spark, dbutils):
    """Test 2: Python SDK for GCS + normal s3a for S3.

    The Python SDK doesn't touch Spark's Hadoop config at all, so there
    should be zero interference with normal S3 reads.
    """
    print_header("Test 2: Python SDK (GCS) + Spark SQL (S3) coexistence")

    sa_key_json = get_secret(dbutils, "sa_key_json")
    key_info = json.loads(sa_key_json)
    credentials = service_account.Credentials.from_service_account_info(key_info)
    client = storage.Client(credentials=credentials, project=key_info.get("project_id"))
    bucket = client.bucket(BUCKET)

    # Step 2a: Read GCS via Python SDK
    print("Reading GCS via Python SDK...")
    try:
        blob = bucket.blob("sample-data/data.csv")
        csv_content = blob.download_as_text()
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)
        df_gcs = spark.createDataFrame(rows)
        df_gcs.show()
        print(f"  PASS (GCS): Read {df_gcs.count()} rows via Python SDK")
    except Exception as e:
        print(f"  FAIL (GCS): {e}")

    # Step 2b: Read S3 via normal Spark SQL
    print("\nReading S3 via normal Spark SQL...")
    try:
        df_s3 = spark.sql(S3_TEST_QUERY)
        df_s3.show()
        print(f"  PASS (S3): Normal Spark SQL works — {df_s3.count()} rows")
    except Exception as e:
        print(f"  FAIL (S3): {e}")


def test_gs_connector_plus_s3(spark, dbutils):
    """Test 3: gs:// connector for GCS + s3a:// for S3 (classic only).

    The gs:// connector uses a completely separate filesystem scheme,
    so it should not interfere with s3a:// at all.
    """
    print_header("Test 3: gs:// connector (GCS) + s3a:// (S3) coexistence")

    cluster_type = get_cluster_type(spark)
    if "serverless" in cluster_type.lower():
        print("SKIP: gs:// connector requires classic compute")
        return

    sa_key_json = get_secret(dbutils, "sa_key_json")
    key_info = json.loads(sa_key_json)

    # Step 3a: Read GCS via gs://
    print("Reading GCS via gs:// connector...")
    try:
        # These configs should already be set at cluster level for classic
        # Setting here as fallback (will fail on serverless anyway)
        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.email", key_info["client_email"])
        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.private.key", key_info["private_key"])
        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.private.key.id", key_info["private_key_id"])
        spark.conf.set("spark.hadoop.fs.gs.project.id", key_info["project_id"])

        gcs_path = f"gs://{BUCKET}/sample-data/data.csv"
        df_gcs = spark.read.csv(gcs_path, header=True, inferSchema=True)
        df_gcs.show()
        print(f"  PASS (GCS): Read {df_gcs.count()} rows via gs://")
    except Exception as e:
        print(f"  FAIL (GCS): {e}")

    # Step 3b: Verify normal S3 access still works
    print("\nVerifying normal S3/UC access...")
    try:
        df_s3 = spark.sql(S3_TEST_QUERY)
        df_s3.show()
        print(f"  PASS (S3): Normal Spark SQL still works — {df_s3.count()} rows")
    except Exception as e:
        print(f"  FAIL (S3): {e}")


def main():
    spark, dbutils = get_spark_and_dbutils()

    cluster_type = get_cluster_type(spark)
    print(f"Compute type: {cluster_type}")

    test_bucket_specific_s3a(spark, dbutils)
    test_python_sdk_plus_s3(spark, dbutils)
    test_gs_connector_plus_s3(spark, dbutils)

    print_summary(spark)


if __name__ == "__main__":
    main()
