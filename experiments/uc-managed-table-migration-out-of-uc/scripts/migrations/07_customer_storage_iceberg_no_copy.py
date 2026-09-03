import json
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _common import (
    GLUE_DATABASE,
    MANAGED_CATALOG,
    athena,
    aws,
    count,
    detail,
    must_athena,
    must_sql,
    summary,
    write_result,
)

SCHEMA = "exit_test"
FQ_SCHEMA = f"`{MANAGED_CATALOG}`.`{SCHEMA}`"


def latest_metadata(location: str) -> str:
    parsed = urlparse(location)
    prefix = f"{parsed.path.lstrip('/')}/_iceberg/metadata/"
    listed = aws(
        "s3api",
        "list-objects-v2",
        "--bucket",
        parsed.netloc,
        "--prefix",
        prefix,
        "--output",
        "json",
    )
    if listed.returncode:
        raise SystemExit(f"Could not list Iceberg metadata: {listed.stderr}")
    candidates = [
        item
        for item in json.loads(listed.stdout).get("Contents", [])
        if item["Key"].endswith("metadata.json")
    ]
    if not candidates:
        raise SystemExit("No Iceberg metadata file found")
    latest = max(candidates, key=lambda item: item["LastModified"])
    return f"s3://{parsed.netloc}/{latest['Key']}"


def main() -> None:
    source = f"{FQ_SCHEMA}.managed_iceberg_no_copy"
    must_sql(f"CREATE TABLE {source} (id INT, value STRING) USING ICEBERG")
    must_sql(f"INSERT INTO {source} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    source_detail = detail(source)
    source_rows = count(source)
    metadata = latest_metadata(source_detail["location"])

    glue_table = "managed_iceberg_no_copy"
    aws("glue", "delete-table", "--database-name", GLUE_DATABASE, "--name", glue_table)
    table_input = {
        "Name": glue_table,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": [{"Name": "id", "Type": "int"}, {"Name": "value", "Type": "string"}],
            "Location": source_detail["location"],
        },
        "Parameters": {"table_type": "ICEBERG", "metadata_location": metadata},
    }
    registered = aws(
        "glue",
        "create-table",
        "--database-name",
        GLUE_DATABASE,
        "--table-input",
        json.dumps(table_input),
    )
    if registered.returncode:
        raise SystemExit(f"Glue registration failed: {registered.stderr}")

    initial_read = must_athena(f"SELECT count(*) FROM {glue_table}")
    update = athena(f"UPDATE {glue_table} SET value='athena-cutover' WHERE id=1")
    destination_read = (
        athena(
            f"SELECT count(*), max(CASE WHEN value='athena-cutover' THEN 1 ELSE 0 END) "
            f"FROM {glue_table}"
        )
        if update.succeeded
        else None
    )
    source_read = must_sql(
        f"SELECT count(*), max(CASE WHEN value='athena-cutover' THEN 1 ELSE 0 END) FROM {source}"
    )
    destination_rows = (
        int(destination_read.rows[0][0])
        if destination_read and destination_read.succeeded
        else None
    )
    destination_write_visible = bool(
        destination_read and destination_read.succeeded and destination_read.rows[0][1] == "1"
    )
    source_write_visible = source_read.rows[0][1] == "1"
    catalog_pointers_diverged = destination_write_visible and not source_write_visible
    status = (
        "pass"
        if int(initial_read.scalar or 0) == source_rows == destination_rows == 3
        and catalog_pointers_diverged
        else "fail"
    )
    write_result(
        "7",
        question=(
            "Can customer-rooted managed Iceberg be registered and cut over to Glue without "
            "copying data?"
        ),
        status=status,
        finding=(
            "Glue/Athena read the UC-managed Iceberg metadata and files in place, then advanced "
            "the destination catalog with a write. Databricks retained its old snapshot, proving "
            "that zero-copy cutover works but concurrent writes create divergent catalog pointers."
            if status == "pass"
            else "The customer-rooted zero-copy Iceberg cutover did not verify as expected."
        ),
        evidence={
            "customer_managed_storage": True,
            "data_copy_required": False,
            "source_format": source_detail.get("format"),
            "source_rows": source_rows,
            "destination_catalog": "AWS Glue",
            "destination_engine": "Amazon Athena",
            "destination_rows": destination_rows,
            "destination_write_verified": destination_write_visible,
            "source_saw_destination_write": source_write_visible,
            "catalog_pointers_diverged_after_destination_write": catalog_pointers_diverged,
            "source_metadata_preserved_at_registration": True,
            "initial_read": summary(initial_read),
            "update": summary(update),
            "destination_read": summary(destination_read) if destination_read else None,
        },
    )
    print(f"row 7: {status}")


if __name__ == "__main__":
    main()
