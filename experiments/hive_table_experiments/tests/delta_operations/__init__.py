"""Comprehensive OPTIMIZE and VACUUM validation tests.

These tests validate Delta Lake maintenance operations:
- OPTIMIZE: File compaction for read performance
- VACUUM: Old version cleanup for storage efficiency

Test Strategy:
1. Create multiple small files via individual INSERTs
2. Count parquet files before (excluding _delta_log/)
3. Run OPTIMIZE and verify file count reduced
4. Run VACUUM and verify old files removed
5. Verify data integrity throughout

Table Properties for Testing:
- delta.logRetentionDuration = 'interval 1 hours'
- delta.deletedFileRetentionDuration = 'interval 1 hours'
- spark.databricks.delta.retentionDurationCheck.enabled = false
"""
