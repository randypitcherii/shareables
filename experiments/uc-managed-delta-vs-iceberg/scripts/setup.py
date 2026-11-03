"""Create the isolated catalog, schema, evidence volume, and seeded source tables.

Idempotent: tables are dropped and recreated (catalog commits cannot be toggled via
CREATE OR REPLACE, so DROP + CREATE is required).
"""

from _common import CATALOG, FQ, SCHEMA, STORAGE_ROOT, VOLUME, create_table_sql, must_sql

# Each dml_* table gets an identical Databricks-side history: insert, update, delete.
SEED = [
    "INSERT INTO {t} VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
    "UPDATE {t} SET v = v + 1 WHERE id = 1",
    "DELETE FROM {t} WHERE id = 2",
]
TABLES = {
    "dml_delta": "delta",  # rows 01 + 02
    "dml_iceberg": "iceberg",  # rows 01 + 02
    "dml_iceberg_clean": "iceberg",  # row 01 control: no Databricks-side UPDATE/DELETE
    "py_iceberg_v3": "iceberg",  # row 05
    "py_iceberg_v2": "iceberg_v2",  # row 05 (v2 control for clients without v3 writes)
    "py_delta": "delta",  # row 05 (Delta with Iceberg reads, via the Iceberg REST catalog)
}


def main() -> None:
    if not STORAGE_ROOT:
        raise SystemExit("EXPERIMENT_STORAGE_ROOT is required (managed location for the catalog)")
    must_sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}` MANAGED LOCATION '{STORAGE_ROOT}'")
    must_sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
    must_sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.`{VOLUME}`")
    me = must_sql("SELECT current_user()").scalar
    # Ownership does not imply EXTERNAL USE SCHEMA; external engines need it explicitly.
    must_sql(f"GRANT EXTERNAL USE SCHEMA ON SCHEMA {FQ} TO `{me}`")
    for name, fmt in TABLES.items():
        t = f"{FQ}.{name}"
        must_sql(f"DROP TABLE IF EXISTS {t}")
        must_sql(create_table_sql(name, fmt))
        for stmt in SEED[:1] if name.endswith("_clean") else SEED:
            must_sql(stmt.format(t=t))
        print(f"OK: {CATALOG}.{SCHEMA}.{name} ({fmt}) seeded -> {must_sql(f'SELECT count(*) FROM {t}').scalar} rows")


if __name__ == "__main__":
    main()
