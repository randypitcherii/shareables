"""
Approach 2: Python google-cloud-storage SDK

Bypasses Spark's filesystem layer entirely. Uses the google-cloud-storage
pip package to read from GCS, then loads data into a Spark DataFrame.

Hypothesis: Most likely to work on serverless since it's just HTTP calls.
"""

import csv
import io
import json

from google.cloud import storage
from google.oauth2 import service_account

from _common import BUCKET, get_secret, get_spark_and_dbutils, print_header, print_summary


def main():
    spark, dbutils = get_spark_and_dbutils()

    sa_key_json = get_secret(dbutils, "sa_key_json")
    print(f"SA key loaded: {'yes' if sa_key_json else 'no'}")

    key_info = json.loads(sa_key_json)
    credentials = service_account.Credentials.from_service_account_info(key_info)
    client = storage.Client(credentials=credentials, project=key_info.get("project_id"))
    bucket = client.bucket(BUCKET)

    # --- Test 1: Read CSV via SDK ---
    print_header("Test 1: Read CSV using google-cloud-storage SDK")

    csv_content = None
    try:
        blob = bucket.blob("sample-data/data.csv")
        csv_content = blob.download_as_text()
        print("Raw CSV content:")
        print(csv_content[:500])
        print("PASS: Read CSV via SDK")
    except Exception as e:
        print(f"FAIL: {e}")

    # --- Test 2: Load into Spark DataFrame ---
    print_header("Test 2: Load into Spark DataFrame")

    try:
        if csv_content:
            reader = csv.DictReader(io.StringIO(csv_content))
            rows = list(reader)
            df = spark.createDataFrame(rows)
            df.show()
            print(f"PASS: Created Spark DataFrame with {df.count()} rows")
        else:
            print("SKIP: No CSV content from previous step")
    except Exception as e:
        print(f"FAIL: {e}")

    # --- Test 3: List bucket contents ---
    print_header("Test 3: List bucket contents")

    try:
        blobs = list(bucket.list_blobs())
        print(f"Bucket contents ({len(blobs)} objects):")
        for b in blobs:
            print(f"  {b.name} ({b.size} bytes)")
        print("PASS: Listed bucket contents")
    except Exception as e:
        print(f"FAIL: {e}")

    print_summary(spark)


if __name__ == "__main__":
    main()
