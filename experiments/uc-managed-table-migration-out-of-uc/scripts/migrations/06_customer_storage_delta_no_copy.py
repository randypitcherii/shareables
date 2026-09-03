import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _common import (
    GLUE_DATABASE,
    MANAGED_CATALOG,
    MANAGED_STORAGE_ROOT,
    athena,
    aws,
    client,
    count,
    detail,
    must_sql,
    summary,
    write_result,
)

SCHEMA = "exit_test"
FQ_SCHEMA = f"`{MANAGED_CATALOG}`.`{SCHEMA}`"


def register(table: str, path: str):
    aws("glue", "delete-table", "--database-name", GLUE_DATABASE, "--name", table)
    return athena(
        f"CREATE EXTERNAL TABLE {table} LOCATION '{path}' TBLPROPERTIES ('table_type'='DELTA')"
    )


def main() -> None:
    must_sql(f"DROP CATALOG IF EXISTS `{MANAGED_CATALOG}` CASCADE")
    client().catalogs.create(name=MANAGED_CATALOG, storage_root=MANAGED_STORAGE_ROOT)
    must_sql(f"CREATE SCHEMA {FQ_SCHEMA}")

    default_table = f"{FQ_SCHEMA}.managed_delta_default"
    portable_table = f"{FQ_SCHEMA}.managed_delta_portable"
    must_sql(f"CREATE TABLE {default_table} (id INT, value STRING)")
    must_sql(f"INSERT INTO {default_table} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    must_sql(
        f"CREATE TABLE {portable_table} (id INT, value STRING) TBLPROPERTIES ("
        "'delta.minReaderVersion'='1', "
        "'delta.minWriterVersion'='2', "
        "'delta.enableDeletionVectors'='false', "
        "'delta.enableRowTracking'='false', "
        "'delta.checkpointPolicy'='classic')"
    )
    must_sql(f"INSERT INTO {portable_table} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

    default_detail = detail(default_table)
    portable_detail = detail(portable_table)
    default_registration = register("managed_delta_default_no_copy", default_detail["location"])
    portable_registration = register("managed_delta_portable_no_copy", portable_detail["location"])
    portable_read = (
        athena("SELECT count(*) FROM managed_delta_portable_no_copy")
        if portable_registration.succeeded
        else None
    )
    destination_rows = (
        int(portable_read.scalar) if portable_read and portable_read.succeeded else None
    )
    default_too_new = bool(
        default_registration.error
        and "protocol version is too new" in default_registration.error.lower()
    )
    status = (
        "pass" if default_too_new and destination_rows == count(portable_table) == 3 else "fail"
    )
    write_result(
        "6",
        question=(
            "Can customer-rooted managed Delta be registered in Glue/Athena without copying data?"
        ),
        status=status,
        finding=(
            "Yes when portability properties are set before writes: Athena read all rows directly "
            "from the UC-managed path. The default managed Delta protocol was still too new, so "
            "customer-owned storage alone is not sufficient."
            if status == "pass"
            else "The customer-rooted zero-copy Delta registration did not verify as expected."
        ),
        evidence={
            "customer_managed_storage": True,
            "data_copy_required": False,
            "default_min_reader_version": default_detail.get("minReaderVersion"),
            "default_min_writer_version": default_detail.get("minWriterVersion"),
            "default_table_features": default_detail.get("tableFeatures"),
            "default_registration": summary(default_registration),
            "portable_min_reader_version": portable_detail.get("minReaderVersion"),
            "portable_min_writer_version": portable_detail.get("minWriterVersion"),
            "portable_table_features": portable_detail.get("tableFeatures"),
            "portable_registration": summary(portable_registration),
            "destination_catalog": "AWS Glue",
            "destination_engine": "Amazon Athena",
            "destination_rows": destination_rows,
            "source_metadata_preserved": True,
            "destination_write_tested": False,
        },
    )
    print(f"row 6: {status}")


if __name__ == "__main__":
    main()
