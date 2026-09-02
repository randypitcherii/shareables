from _common import FQ_SCHEMA, GLUE_DATABASE, athena, aws, must_sql

if __name__ == "__main__":
    must_sql(f"DROP SCHEMA IF EXISTS {FQ_SCHEMA} CASCADE")
    for table in (
        "default_delta_export",
        "portable_delta_export",
        "iceberg_stage",
        "managed_iceberg_export",
    ):
        athena(f"DROP TABLE IF EXISTS {table}")
    aws("glue", "delete-database", "--name", GLUE_DATABASE)
    print(
        "OK: experiment UC schema and Glue database dropped; object-storage files remain "
        "for explicit storage-owner cleanup"
    )
