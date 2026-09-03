"""Create the S3-rooted UC catalog and both managed source tables."""

from _common import FQ_SCHEMA, MANAGED_CATALOG, MANAGED_STORAGE_ROOT, client, count, must_sql, sql

ROWS = "VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')"


def main() -> None:
    sql(f"DROP CATALOG IF EXISTS `{MANAGED_CATALOG}` CASCADE")
    client().catalogs.create(
        name=MANAGED_CATALOG,
        storage_root=MANAGED_STORAGE_ROOT,
        comment="Temporary managed-table exit experiment; safe to drop",
    )
    must_sql(f"CREATE SCHEMA {FQ_SCHEMA}")
    must_sql(
        f"CREATE TABLE {FQ_SCHEMA}.managed_delta (id INT, value STRING) "
        "TBLPROPERTIES ('delta.universalFormat.enabledFormats'='iceberg', "
        "'delta.enableIcebergCompatV2'='true')"
    )
    must_sql(f"INSERT INTO {FQ_SCHEMA}.managed_delta {ROWS}")
    must_sql(f"CREATE TABLE {FQ_SCHEMA}.managed_iceberg (id INT, value STRING) USING ICEBERG")
    must_sql(f"INSERT INTO {FQ_SCHEMA}.managed_iceberg {ROWS}")
    for name in ("managed_delta", "managed_iceberg"):
        if count(f"{FQ_SCHEMA}.{name}") != 3:
            raise SystemExit(f"{name} did not contain three rows")
    print("OK: S3-rooted catalog created; managed Delta (UniForm) and managed Iceberg hold 3 rows")


if __name__ == "__main__":
    main()
