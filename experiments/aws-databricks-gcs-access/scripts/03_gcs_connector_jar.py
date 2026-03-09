"""
Approach 3: GCS Connector JAR (Hadoop FileSystem)

Uses Spark Hadoop config to register gs:// as a filesystem.
Requires the gcs-connector JAR on the cluster.

Hypothesis: Will NOT work on serverless (custom JARs not supported).
Should work on classic interactive and job clusters.
"""

import json

from _common import (
    BUCKET,
    get_cluster_type,
    get_secret,
    get_spark_and_dbutils,
    print_header,
    print_summary,
)


def main():
    spark, dbutils = get_spark_and_dbutils()

    cluster_type = get_cluster_type(spark)
    print(f"Compute type: {cluster_type}")

    if "serverless" in cluster_type.lower():
        print("WARNING: This approach requires custom JARs and will likely NOT work on serverless.")
        print("Proceeding anyway to document the failure mode...")

    sa_key_json = get_secret(dbutils, "sa_key_json")
    key_info = json.loads(sa_key_json)

    # --- Test 1: Configure Spark Hadoop settings for gs:// ---
    print_header("Test 1: Read via gs:// with Hadoop config")

    try:
        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.email", key_info["client_email"])
        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.private.key", key_info["private_key"])
        spark.conf.set("spark.hadoop.google.cloud.auth.service.account.private.key.id", key_info["private_key_id"])
        spark.conf.set("spark.hadoop.fs.gs.project.id", key_info["project_id"])
        spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.bigdataoss.gcs.GoogleHadoopFileSystem")
        spark.conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.bigdataoss.gcs.GoogleHadoopFS")
        print("Spark Hadoop configs set for GCS connector")

        gcs_path = f"gs://{BUCKET}/sample-data/data.csv"
        df = spark.read.csv(gcs_path, header=True, inferSchema=True)
        df.show()
        print(f"PASS: Read {df.count()} rows via gs:// path")
    except Exception as e:
        print(f"FAIL: {e}")
        if "serverless" in cluster_type.lower():
            print("Expected on serverless - GCS Connector JAR must be installed on classic compute.")

    print_summary(spark)


if __name__ == "__main__":
    main()
