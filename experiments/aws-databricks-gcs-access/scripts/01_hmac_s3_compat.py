"""
Approach 1: HMAC/S3-Compatible GCS Access

Uses GCS's S3-compatible interoperability API with HMAC keys.
Databricks treats GCS as if it were an S3 endpoint via s3a://.

Hypothesis: May not work on serverless if spark.hadoop.fs.s3a.* overrides are blocked.
"""

from _common import BUCKET, get_secret, get_spark_and_dbutils, print_header, print_summary


def main():
    spark, dbutils = get_spark_and_dbutils()

    hmac_access_id = get_secret(dbutils, "hmac_access_id")
    hmac_secret = get_secret(dbutils, "hmac_secret")
    print(f"HMAC Access ID loaded: {'yes' if hmac_access_id else 'no'}")

    gcs_s3_path = f"s3a://{BUCKET}/sample-data/data.csv"
    print(f"Target path: {gcs_s3_path}")

    # --- Test 1: Basic HMAC config ---
    print_header("Test 1: Basic HMAC/S3-compatible access")

    try:
        spark.conf.set("spark.hadoop.fs.s3a.endpoint", "https://storage.googleapis.com")
        spark.conf.set("spark.hadoop.fs.s3a.access.key", hmac_access_id)
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", hmac_secret)

        df = spark.read.csv(gcs_s3_path, header=True, inferSchema=True)
        df.show()
        print(f"PASS: Read {df.count()} rows via HMAC/S3-compatible path")
    except Exception as e:
        print(f"FAIL: {e}")

    # --- Test 2: With path-style access ---
    print_header("Test 2: HMAC with path-style access")

    try:
        spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

        df = spark.read.csv(gcs_s3_path, header=True, inferSchema=True)
        df.show()
        print(f"PASS: Read {df.count()} rows via HMAC with path-style access")
    except Exception as e:
        print(f"FAIL: {e}")

    print_summary(spark)


if __name__ == "__main__":
    main()
