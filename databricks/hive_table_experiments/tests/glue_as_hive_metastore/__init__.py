"""Tests for glue_as_hive_metastore access method.

This access method uses Spark's built-in Glue integration via the hive_metastore catalog.
Tables are accessed as: hive_metastore.{database}.{table}

Capabilities:
- Full partition discovery (including external partitions)
- Cross-bucket partition support
- INSERT operations
- NO UPDATE/DELETE (external Hive tables)
- NO OPTIMIZE/VACUUM (not applicable)
"""
