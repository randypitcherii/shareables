import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _common import (
    EXTERNAL_ROOT,
    FQ_SCHEMA,
    athena,
    clear_external_path,
    count,
    detail,
    must_athena,
    sql,
    summary,
    write_result,
)


def main() -> None:
    source = f"{FQ_SCHEMA}.managed_iceberg"
    staging_table = f"{FQ_SCHEMA}.iceberg_staging_parquet"
    staging_path = f"{EXTERNAL_ROOT}/iceberg_staging_parquet"
    destination_path = f"{EXTERNAL_ROOT}/glue_iceberg"
    source_rows = count(source)
    source_format = detail(source).get("format")

    sql(f"DROP TABLE IF EXISTS {staging_table}")
    clear_external_path(staging_path)
    stage = sql(
        f"CREATE TABLE {staging_table} USING PARQUET LOCATION '{staging_path}' "
        f"AS SELECT * FROM {source}"
    )
    must_athena("DROP TABLE IF EXISTS iceberg_stage")
    must_athena("DROP TABLE IF EXISTS managed_iceberg_export")
    clear_external_path(destination_path)
    if stage.succeeded:
        must_athena(
            "CREATE EXTERNAL TABLE iceberg_stage (id int, value string) "
            f"STORED AS PARQUET LOCATION '{staging_path}'"
        )
        create = athena(
            "CREATE TABLE managed_iceberg_export WITH ("
            "table_type='ICEBERG', format='PARQUET', "
            f"location='{destination_path}', is_external=false) AS SELECT * FROM iceberg_stage"
        )
    else:
        create = None
    update = (
        athena("UPDATE managed_iceberg_export SET value='athena-write' WHERE id=1")
        if create and create.succeeded
        else None
    )
    verify = (
        athena(
            "SELECT count(*), max(CASE WHEN value='athena-write' THEN 1 ELSE 0 END) "
            "FROM managed_iceberg_export"
        )
        if update and update.succeeded
        else None
    )
    destination_rows = int(verify.rows[0][0]) if verify and verify.succeeded else None
    write_verified = verify.rows[0][1] == "1" if verify and verify.succeeded else False
    status = "pass" if destination_rows == source_rows == 3 and write_verified else "fail"
    finding = (
        "A Parquet staging copy moved all rows from managed Iceberg into AWS Glue Iceberg; "
        "Athena then updated and reread the destination. The migration copied data and did "
        "not preserve the source table metadata."
        if status == "pass"
        else "The managed-Iceberg to Glue-Iceberg copy did not complete and verify end to end."
    )
    write_result(
        "4",
        question="Can managed Iceberg be migrated to a non-Databricks Iceberg catalog?",
        status=status,
        finding=finding,
        evidence={
            "source_format": source_format,
            "source_rows": source_rows,
            "staging_copy": summary(stage),
            "destination_catalog": "AWS Glue",
            "destination_engine": "Amazon Athena",
            "destination_format": "iceberg",
            "destination_rows": destination_rows,
            "destination_write_verified": write_verified,
            "data_copy_required": True,
            "source_metadata_preserved": False,
            "create": summary(create) if create else None,
            "update": summary(update) if update else None,
            "verify": summary(verify) if verify else None,
        },
    )
    print(f"row 4: {status}")


if __name__ == "__main__":
    main()
