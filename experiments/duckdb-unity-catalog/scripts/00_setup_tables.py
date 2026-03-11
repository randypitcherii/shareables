"""Set up test tables in Unity Catalog for the DuckDB experiment.

Creates:
  - managed_delta: Managed Delta table with sample data
  - external_delta: External Delta table with sample data
  - managed_iceberg: Managed Iceberg table (UniForm enabled) with sample data

All tables live in fe_randy_pitcher_workspace_catalog.duckdb_uc_experiment.
Uses samples.nyctaxi.trips as source data (100 rows).
"""

from _common import CATALOG, SCHEMA, FULL_SCHEMA, get_spark, print_header, print_result


def main():
    spark = get_spark()

    # --- Create schema ---
    print_header("Creating experiment schema")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
    print_result("CREATE SCHEMA", True, FULL_SCHEMA)

    # --- Source data ---
    source = "samples.nyctaxi.trips"
    row_limit = 100

    # --- Managed Delta ---
    table = f"{FULL_SCHEMA}.managed_delta"
    print_header(f"Creating {table}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"""
        CREATE TABLE {table}
        USING DELTA
        AS SELECT * FROM {source} LIMIT {row_limit}
    """)
    count = spark.sql(f"SELECT count(*) as cnt FROM {table}").collect()[0]["cnt"]
    print_result("managed_delta", True, f"{count} rows")

    # --- External Delta ---
    # Use a managed location path for external — write data first, then create external table pointing to it
    ext_table = f"{FULL_SCHEMA}.external_delta"
    print_header(f"Creating {ext_table}")
    spark.sql(f"DROP TABLE IF EXISTS {ext_table}")
    # For external delta, we create it as a regular table first to get data placed,
    # then note: on serverless UC, truly "external" tables require an external location.
    # Instead, we'll create a standard delta table and mark it in our grid notes.
    # The key difference for DuckDB is whether it can access the table — both managed
    # and "external" delta tables surface the same way through UC REST.
    spark.sql(f"""
        CREATE TABLE {ext_table}
        USING DELTA
        AS SELECT * FROM {source} LIMIT {row_limit}
    """)
    count = spark.sql(f"SELECT count(*) as cnt FROM {ext_table}").collect()[0]["cnt"]
    print_result("external_delta", True, f"{count} rows (note: created as managed, see README)")

    # --- Managed Iceberg (UniForm) ---
    ice_table = f"{FULL_SCHEMA}.managed_iceberg"
    print_header(f"Creating {ice_table}")
    spark.sql(f"DROP TABLE IF EXISTS {ice_table}")
    spark.sql(f"""
        CREATE TABLE {ice_table}
        USING DELTA
        TBLPROPERTIES (
            'delta.universalFormat.enabledFormats' = 'iceberg',
            'delta.enableIcebergCompatV2' = 'true'
        )
        AS SELECT * FROM {source} LIMIT {row_limit}
    """)
    count = spark.sql(f"SELECT count(*) as cnt FROM {ice_table}").collect()[0]["cnt"]
    print_result("managed_iceberg (UniForm)", True, f"{count} rows")

    # --- Verify all tables ---
    print_header("Verification — listing tables")
    tables = spark.sql(f"SHOW TABLES IN {FULL_SCHEMA}").collect()
    for t in tables:
        print(f"  📋 {t['tableName']}")

    print_header("Setup complete")
    print(f"  Schema: {FULL_SCHEMA}")
    print(f"  Tables: managed_delta, external_delta, managed_iceberg")
    print(f"  Source: {source} ({row_limit} rows each)")


if __name__ == "__main__":
    main()
