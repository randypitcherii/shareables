"""Tests for uc_glue_federation access method.

This access method uses Unity Catalog federation to access Glue tables.
Tables are accessed as: {uc_glue_catalog}.{database}.{table}

Capabilities:
- Inherits Glue's partition discovery
- Cross-bucket partition support
- READ-ONLY (federation does not support writes)
- NO UPDATE/DELETE
- NO OPTIMIZE/VACUUM
"""
