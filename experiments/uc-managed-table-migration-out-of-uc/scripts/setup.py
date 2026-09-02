from _common import FQ_SCHEMA, must_sql, sql

ROWS = "VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')"


def main() -> None:
    must_sql(f"CREATE SCHEMA IF NOT EXISTS {FQ_SCHEMA}")
    must_sql(f"CREATE OR REPLACE TABLE {FQ_SCHEMA}.managed_delta (id INT, value STRING)")
    must_sql(f"INSERT INTO {FQ_SCHEMA}.managed_delta {ROWS}")
    iceberg = sql(
        f"CREATE OR REPLACE TABLE {FQ_SCHEMA}.managed_iceberg (id INT, value STRING) USING ICEBERG"
    )
    if iceberg.succeeded:
        must_sql(f"INSERT INTO {FQ_SCHEMA}.managed_iceberg {ROWS}")
        print("OK: managed Delta and managed Iceberg sources contain three rows each")
    else:
        print(f"NOTE: managed Iceberg source unavailable: {iceberg.error_code}: {iceberg.error}")


if __name__ == "__main__":
    main()
