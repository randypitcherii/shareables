import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _common import (
    EXTERNAL_ROOT,
    FQ_SCHEMA,
    GLUE_DATABASE,
    athena,
    aws,
    clear_external_path,
    count,
    detail,
    sql,
    summary,
    write_result,
)


def register(table: str, path: str):
    aws("glue", "delete-table", "--database-name", GLUE_DATABASE, "--name", table)
    return athena(
        f"CREATE EXTERNAL TABLE {table} LOCATION '{path}' TBLPROPERTIES ('table_type'='DELTA')"
    )


def main() -> None:
    source = f"{FQ_SCHEMA}.managed_delta"
    default_path = f"{EXTERNAL_ROOT}/uncataloged_delta"
    portable_path = f"{EXTERNAL_ROOT}/portable_delta"
    destination = f"delta.`{portable_path}`"

    default_registration = register("default_delta_export", default_path)
    sql(f"DROP TABLE IF EXISTS {destination}")
    clear_external_path(portable_path)
    portable_copy = sql(
        f"CREATE TABLE {destination} TBLPROPERTIES ("
        "'delta.minReaderVersion'='1', "
        "'delta.minWriterVersion'='2', "
        "'delta.enableDeletionVectors'='false', "
        "'delta.checkpointPolicy'='classic') "
        f"AS SELECT * FROM {source}"
    )
    portable_detail = detail(destination) if portable_copy.succeeded else {}
    portable_registration = (
        register("portable_delta_export", portable_path) if portable_copy.succeeded else None
    )
    verify = (
        athena("SELECT count(*) FROM portable_delta_export")
        if portable_registration and portable_registration.succeeded
        else None
    )
    destination_rows = int(verify.scalar) if verify and verify.succeeded else None
    source_rows = count(source)
    default_too_new = bool(
        default_registration.error
        and "protocol version is too new" in default_registration.error.lower()
    )
    status = "pass" if destination_rows == source_rows == 3 and default_too_new else "fail"
    finding = (
        "AWS Glue/Athena rejected the default Databricks Delta protocol, but registered and "
        "read a copied Delta table written with reader version 1, writer version 2, classic "
        "checkpoints, and deletion vectors disabled."
        if status == "pass"
        else "The portable Delta copy was not registered and read by AWS Glue/Athena as expected."
    )
    write_result(
        "5",
        question="Can managed Delta be copied and registered in AWS Glue for Athena reads?",
        status=status,
        finding=finding,
        evidence={
            "source_rows": source_rows,
            "default_protocol_registration": summary(default_registration),
            "portable_copy": summary(portable_copy),
            "portable_min_reader_version": portable_detail.get("minReaderVersion"),
            "portable_min_writer_version": portable_detail.get("minWriterVersion"),
            "portable_table_features": portable_detail.get("tableFeatures"),
            "destination_catalog": "AWS Glue",
            "destination_engine": "Amazon Athena",
            "destination_rows": destination_rows,
            "data_copy_required": True,
            "source_metadata_preserved": False,
            "portable_registration": (
                summary(portable_registration) if portable_registration else None
            ),
            "verify": summary(verify) if verify else None,
        },
    )
    print(f"row 5: {status}")


if __name__ == "__main__":
    main()
