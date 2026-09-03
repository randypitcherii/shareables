"""Live preflight: identity, warehouse, external location, and AWS credentials."""

import boto3
from _common import AWS_REGION, EXTERNAL_ROOT, MANAGED_STORAGE_ROOT, client, must_sql


def main() -> None:
    identity = must_sql("SELECT current_user(), current_version().dbsql_version").rows[0]
    locations = list(client().external_locations.list())
    matching = [
        location
        for location in locations
        if EXTERNAL_ROOT.startswith((location.url or "").rstrip("/"))
    ]
    if not matching:
        raise SystemExit("EXPERIMENT_EXTERNAL_ROOT is not under a visible UC external location")
    if matching[0].read_only:
        raise SystemExit("The matching UC external location is read-only")
    if not MANAGED_STORAGE_ROOT.startswith(EXTERNAL_ROOT + "/"):
        raise SystemExit("Managed storage root must sit inside EXPERIMENT_EXTERNAL_ROOT")
    account = boto3.client("sts", region_name=AWS_REGION).get_caller_identity()["Account"]
    bucket = EXTERNAL_ROOT.removeprefix("s3://").split("/", 1)[0]
    boto3.client("s3", region_name=AWS_REGION).head_bucket(Bucket=bucket)
    print(f"OK: authenticated as {identity[0]} on DBSQL {identity[1]}")
    print("OK: external root is governed by a writable UC external location")
    print(f"OK: AWS account …{account[-4:]} can reach the experiment bucket directly")


if __name__ == "__main__":
    main()
