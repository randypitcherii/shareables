"""
Approach 5: Cloudflare R2 as Cross-Cloud Storage Intermediary

Cloudflare R2 is S3-compatible and could serve as a neutral intermediary
between clouds. This tests whether AWS Databricks can read from R2 using
the s3a:// filesystem with R2-specific credentials and endpoint.

Requires R2 credentials stored in Databricks secrets:
  - r2_access_key_id: R2 API token access key
  - r2_secret_access_key: R2 API token secret key
  - r2_account_id: Cloudflare account ID
  - r2_bucket_name: R2 bucket name

Setup:
  1. Create R2 bucket in Cloudflare dashboard
  2. Create R2 API token with read access
  3. Upload test data: sample-data/data.csv (same schema as GCS experiment)
  4. Store credentials in Databricks secret scope 'r2-experiment':
     databricks secrets create-scope r2-experiment
     databricks secrets put-secret r2-experiment r2_access_key_id --string-value <value>
     databricks secrets put-secret r2-experiment r2_secret_access_key --string-value <value>
     databricks secrets put-secret r2-experiment r2_account_id --string-value <value>
     databricks secrets put-secret r2-experiment r2_bucket_name --string-value <value>

Tests:
  1. Global s3a config pointing to R2 endpoint
  2. Bucket-specific s3a config (R2 isolated, normal S3 untouched)
  3. Mixed R2 + GCS + S3 access in same session (the holy grail)
"""

from _common import (
    BUCKET as GCS_BUCKET,
    get_secret,
    get_spark_and_dbutils,
    print_header,
    print_summary,
)

R2_SCOPE = "r2-experiment"


def get_r2_config(dbutils):
    """Load R2 credentials from Databricks secrets."""
    account_id = dbutils.secrets.get(scope=R2_SCOPE, key="r2_account_id")
    return {
        "endpoint": f"https://{account_id}.r2.cloudflarestorage.com",
        "access_key": dbutils.secrets.get(scope=R2_SCOPE, key="r2_access_key_id"),
        "secret_key": dbutils.secrets.get(scope=R2_SCOPE, key="r2_secret_access_key"),
        "bucket": dbutils.secrets.get(scope=R2_SCOPE, key="r2_bucket_name"),
    }


def test_global_r2_access(spark, r2_config):
    """Test 1: Global s3a config pointing to R2.

    Simple approach — overrides all s3a settings to point to R2.
    Will break normal S3 access (same limitation as GCS HMAC approach).
    """
    print_header("Test 1: Global s3a config → R2")

    r2_path = f"s3a://{r2_config['bucket']}/sample-data/data.csv"
    print(f"Target: {r2_path}")

    try:
        spark.conf.set("spark.hadoop.fs.s3a.endpoint", r2_config["endpoint"])
        spark.conf.set("spark.hadoop.fs.s3a.access.key", r2_config["access_key"])
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", r2_config["secret_key"])
        spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

        df = spark.read.csv(r2_path, header=True, inferSchema=True)
        df.show()
        print(f"PASS: Read {df.count()} rows from R2 via global s3a config")
    except Exception as e:
        print(f"FAIL: {e}")

    # Reset global config to avoid polluting subsequent tests
    try:
        spark.conf.unset("spark.hadoop.fs.s3a.endpoint")
        spark.conf.unset("spark.hadoop.fs.s3a.path.style.access")
    except Exception:
        pass


def test_bucket_specific_r2(spark, r2_config):
    """Test 2: Bucket-specific s3a config for R2.

    Uses per-bucket Hadoop config so R2 creds only apply to the R2 bucket,
    leaving default s3a config intact for normal AWS S3.
    """
    print_header("Test 2: Bucket-specific s3a config for R2")

    bucket = r2_config["bucket"]
    r2_path = f"s3a://{bucket}/sample-data/data.csv"

    try:
        prefix = f"spark.hadoop.fs.s3a.bucket.{bucket}"
        spark.conf.set(f"{prefix}.endpoint", r2_config["endpoint"])
        spark.conf.set(f"{prefix}.access.key", r2_config["access_key"])
        spark.conf.set(f"{prefix}.secret.key", r2_config["secret_key"])
        spark.conf.set(f"{prefix}.path.style.access", "true")
        print(f"Per-bucket config set for: {bucket}")

        df_r2 = spark.read.csv(r2_path, header=True, inferSchema=True)
        df_r2.show()
        print(f"PASS (R2): Read {df_r2.count()} rows via bucket-specific config")
    except Exception as e:
        print(f"FAIL (R2): {e}")

    # Verify normal S3/UC access
    print("\nVerifying normal S3/UC access is not broken...")
    try:
        df_s3 = spark.sql("SELECT 1 AS id, 'hello from S3' AS source")
        df_s3.show()
        print(f"PASS (S3): Normal Spark SQL still works — {df_s3.count()} rows")
    except Exception as e:
        print(f"FAIL (S3): {e}")


def test_mixed_r2_gcs_s3(spark, dbutils, r2_config):
    """Test 3: Mixed R2 + GCS + S3 in the same session.

    The holy grail — can we access all three storage backends simultaneously?
    Uses bucket-specific s3a for both R2 and GCS, plus normal S3 for UC.
    """
    print_header("Test 3: Mixed R2 + GCS + S3 in same session")

    hmac_access_id = get_secret(dbutils, "hmac_access_id")
    hmac_secret = get_secret(dbutils, "hmac_secret")

    r2_bucket = r2_config["bucket"]
    r2_path = f"s3a://{r2_bucket}/sample-data/data.csv"
    gcs_path = f"s3a://{GCS_BUCKET}/sample-data/data.csv"

    # Configure R2 bucket-specific
    r2_prefix = f"spark.hadoop.fs.s3a.bucket.{r2_bucket}"
    spark.conf.set(f"{r2_prefix}.endpoint", r2_config["endpoint"])
    spark.conf.set(f"{r2_prefix}.access.key", r2_config["access_key"])
    spark.conf.set(f"{r2_prefix}.secret.key", r2_config["secret_key"])
    spark.conf.set(f"{r2_prefix}.path.style.access", "true")

    # Configure GCS bucket-specific
    gcs_prefix = f"spark.hadoop.fs.s3a.bucket.{GCS_BUCKET}"
    spark.conf.set(f"{gcs_prefix}.endpoint", "https://storage.googleapis.com")
    spark.conf.set(f"{gcs_prefix}.access.key", hmac_access_id)
    spark.conf.set(f"{gcs_prefix}.secret.key", hmac_secret)
    spark.conf.set(f"{gcs_prefix}.path.style.access", "true")

    print("Bucket-specific configs set for both R2 and GCS")

    # Read from R2
    print("\nReading from R2...")
    try:
        df_r2 = spark.read.csv(r2_path, header=True, inferSchema=True)
        df_r2.show()
        print(f"PASS (R2): Read {df_r2.count()} rows")
    except Exception as e:
        print(f"FAIL (R2): {e}")

    # Read from GCS
    print("\nReading from GCS...")
    try:
        df_gcs = spark.read.csv(gcs_path, header=True, inferSchema=True)
        df_gcs.show()
        print(f"PASS (GCS): Read {df_gcs.count()} rows")
    except Exception as e:
        print(f"FAIL (GCS): {e}")

    # Read from S3 (via Spark SQL / UC)
    print("\nReading from S3/UC...")
    try:
        df_s3 = spark.sql("SELECT 1 AS id, 'hello from S3' AS source")
        df_s3.show()
        print(f"PASS (S3): Normal Spark SQL works — {df_s3.count()} rows")
    except Exception as e:
        print(f"FAIL (S3): {e}")


def main():
    spark, dbutils = get_spark_and_dbutils()

    # Check if R2 secrets exist
    print_header("Checking R2 credentials")
    try:
        r2_config = get_r2_config(dbutils)
        print(f"R2 endpoint: {r2_config['endpoint']}")
        print(f"R2 bucket: {r2_config['bucket']}")
        print(f"R2 access key loaded: {'yes' if r2_config['access_key'] else 'no'}")
    except Exception as e:
        print(f"R2 credentials not found: {e}")
        print("\nTo set up R2 credentials, run:")
        print("  databricks secrets create-scope r2-experiment")
        print("  databricks secrets put-secret r2-experiment r2_account_id --string-value <value>")
        print("  databricks secrets put-secret r2-experiment r2_access_key_id --string-value <value>")
        print("  databricks secrets put-secret r2-experiment r2_secret_access_key --string-value <value>")
        print("  databricks secrets put-secret r2-experiment r2_bucket_name --string-value <value>")
        print("\nThen upload test data to R2: sample-data/data.csv")
        print_summary(spark)
        return

    test_global_r2_access(spark, r2_config)
    test_bucket_specific_r2(spark, r2_config)
    test_mixed_r2_gcs_s3(spark, dbutils, r2_config)

    print_summary(spark)


if __name__ == "__main__":
    main()
