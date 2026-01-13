"""Tests for uc_external_tables access method.

This access method creates manual external tables in Unity Catalog pointing to S3.
Two variants are tested:
1. uc_ext (Parquet) - External Hive-format tables
2. uc_delta (Delta) - External Delta tables with full ACID support

Capabilities vary by format:
- uc_ext: INSERT only, no UPDATE/DELETE/OPTIMIZE/VACUUM
- uc_delta: Full ACID (INSERT, UPDATE, DELETE, OPTIMIZE, VACUUM)

IMPORTANT: UC external tables do NOT support external partitions.
Scenarios with partitions outside the table root require data consolidation.
"""
