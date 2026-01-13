"""
hive_to_delta - Bulk registration of AWS Glue Hive tables as external Delta tables in Unity Catalog.

Usage:
    from hive_to_delta import convert_tables, vacuum_external_files, list_glue_tables

    results = convert_tables(
        spark=spark,
        glue_database="my_glue_db",
        tables=["table1", "table2"],
        target_catalog="unity_catalog",
        target_schema="migrated_tables",
        aws_region="us-east-1",
        max_workers=4,
    )
"""

from hive_to_delta.converter import convert_single_table, convert_tables
from hive_to_delta.glue import list_glue_tables
from hive_to_delta.vacuum import vacuum_external_files

__all__ = ["convert_single_table", "convert_tables", "vacuum_external_files", "list_glue_tables"]
__version__ = "0.1.0"
