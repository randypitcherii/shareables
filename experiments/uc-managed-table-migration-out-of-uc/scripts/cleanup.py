from _common import FQ_SCHEMA, GLUE_DATABASE, MANAGED_CATALOG, athena, aws, must_sql

if __name__ == "__main__":
    must_sql(f"DROP SCHEMA IF EXISTS {FQ_SCHEMA} CASCADE")
    must_sql(f"DROP CATALOG IF EXISTS `{MANAGED_CATALOG}` CASCADE")
    for table in (
        "default_delta_export",
        "portable_delta_export",
        "iceberg_stage",
        "managed_iceberg_export",
        "managed_delta_default_no_copy",
        "managed_delta_portable_no_copy",
        "managed_iceberg_no_copy",
    ):
        athena(f"DROP TABLE IF EXISTS {table}")
    aws("glue", "delete-database", "--name", GLUE_DATABASE)
    print(
        "OK: experiment UC schema and Glue database dropped; object-storage files remain "
        "for explicit storage-owner cleanup"
    )
