"""Tests for vacuum_external_files functionality.

These tests verify that the external vacuum utility correctly identifies
and deletes orphaned files at absolute S3 paths that standard VACUUM
cannot reach.
"""

import time
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from hive_to_delta import vacuum_external_files
from hive_to_delta.models import VacuumResult
from hive_to_delta.vacuum import (
    find_orphaned_external_paths,
    parse_add_actions,
    parse_remove_actions,
)


class TestVacuumExternalDryRun:
    """Test dry run mode finds orphaned files without deleting."""

    @pytest.mark.standard
    def test_vacuum_external_dry_run_finds_orphaned_files(self):
        """Verify dry run identifies orphaned external files without deletion."""
        table_location = "s3://main-bucket/tables/my_table"
        external_bucket = "s3://external-bucket/data"

        # Simulate Delta log entries: one file added then removed
        old_timestamp = int((datetime.now().timestamp() - 8 * 24 * 60 * 60) * 1000)  # 8 days ago

        entries = [
            {
                "add": {
                    "path": f"{external_bucket}/file1.parquet",
                    "partitionValues": {},
                    "size": 1000,
                },
                "_version": 0,
            },
            {
                "remove": {
                    "path": f"{external_bucket}/file1.parquet",
                    "deletionTimestamp": old_timestamp,
                    "size": 1000,
                },
                "_version": 1,
            },
        ]

        removes = parse_remove_actions(entries)
        adds = parse_add_actions(entries)

        orphaned = find_orphaned_external_paths(
            removes=removes,
            adds=adds,
            table_location=table_location,
            retention_hours=168,  # 7 days
        )

        assert len(orphaned) == 1
        assert orphaned[0] == f"{external_bucket}/file1.parquet"

    @pytest.mark.standard
    def test_vacuum_external_dry_run_returns_result(self):
        """Verify dry run returns VacuumResult with orphaned files but no deletions."""
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"location": "s3://bucket/table"}
        ]

        old_timestamp = int((datetime.now().timestamp() - 8 * 24 * 60 * 60) * 1000)

        mock_entries = [
            {
                "remove": {
                    "path": "s3://external/orphan.parquet",
                    "deletionTimestamp": old_timestamp,
                },
                "_version": 1,
            },
        ]

        with patch("hive_to_delta.vacuum.get_delta_log_entries", return_value=mock_entries):
            result = vacuum_external_files(
                spark=mock_spark,
                table_name="catalog.schema.table",
                retention_hours=168,
                dry_run=True,
            )

        assert isinstance(result, VacuumResult)
        assert result.dry_run is True
        assert result.deleted_files == []
        assert "s3://external/orphan.parquet" in result.orphaned_files

    @pytest.mark.standard
    def test_vacuum_external_dry_run_skips_active_files(self):
        """Verify dry run does not flag files that are currently active."""
        table_location = "s3://main-bucket/tables/my_table"
        external_path = "s3://external-bucket/data/active.parquet"

        old_timestamp = int((datetime.now().timestamp() - 8 * 24 * 60 * 60) * 1000)

        # File removed then re-added (active)
        entries = [
            {
                "add": {"path": external_path, "partitionValues": {}, "size": 1000},
                "_version": 0,
            },
            {
                "remove": {"path": external_path, "deletionTimestamp": old_timestamp},
                "_version": 1,
            },
            {
                "add": {"path": external_path, "partitionValues": {}, "size": 1000},
                "_version": 2,
            },
        ]

        removes = parse_remove_actions(entries)
        adds = parse_add_actions(entries)

        orphaned = find_orphaned_external_paths(
            removes=removes,
            adds=adds,
            table_location=table_location,
            retention_hours=168,
        )

        assert len(orphaned) == 0


class TestVacuumExternalDelete:
    """Test actual deletion of orphaned external files."""

    @pytest.mark.standard
    def test_vacuum_external_delete_removes_files(self):
        """Verify actual deletion mode removes orphaned files."""
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"location": "s3://bucket/table"}
        ]

        old_timestamp = int((datetime.now().timestamp() - 8 * 24 * 60 * 60) * 1000)

        mock_entries = [
            {
                "remove": {
                    "path": "s3://external/orphan.parquet",
                    "deletionTimestamp": old_timestamp,
                },
                "_version": 1,
            },
        ]

        mock_delete_response = {
            "Deleted": [{"Key": "orphan.parquet"}],
            "Errors": [],
        }

        with (
            patch("hive_to_delta.vacuum.get_delta_log_entries", return_value=mock_entries),
            patch("hive_to_delta.vacuum.get_s3_client") as mock_s3,
        ):
            mock_s3.return_value.delete_objects.return_value = mock_delete_response

            result = vacuum_external_files(
                spark=mock_spark,
                table_name="catalog.schema.table",
                retention_hours=168,
                dry_run=False,
            )

        assert result.dry_run is False
        assert len(result.deleted_files) > 0
        mock_s3.return_value.delete_objects.assert_called_once()

    @pytest.mark.standard
    def test_vacuum_external_delete_handles_multiple_buckets(self):
        """Verify deletion works across multiple external buckets."""
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"location": "s3://main/table"}
        ]

        old_timestamp = int((datetime.now().timestamp() - 8 * 24 * 60 * 60) * 1000)

        mock_entries = [
            {
                "remove": {
                    "path": "s3://bucket-a/file1.parquet",
                    "deletionTimestamp": old_timestamp,
                },
                "_version": 1,
            },
            {
                "remove": {
                    "path": "s3://bucket-b/file2.parquet",
                    "deletionTimestamp": old_timestamp,
                },
                "_version": 2,
            },
        ]

        with (
            patch("hive_to_delta.vacuum.get_delta_log_entries", return_value=mock_entries),
            patch("hive_to_delta.vacuum.get_s3_client") as mock_s3,
        ):
            # Simulate successful deletions from both buckets
            mock_s3.return_value.delete_objects.side_effect = [
                {"Deleted": [{"Key": "file1.parquet"}], "Errors": []},
                {"Deleted": [{"Key": "file2.parquet"}], "Errors": []},
            ]

            result = vacuum_external_files(
                spark=mock_spark,
                table_name="catalog.schema.table",
                retention_hours=168,
                dry_run=False,
            )

        assert result.dry_run is False
        assert len(result.orphaned_files) == 2
        # delete_objects called once per bucket
        assert mock_s3.return_value.delete_objects.call_count == 2


class TestVacuumRespectsRetention:
    """Test that vacuum respects the retention period."""

    @pytest.mark.standard
    def test_vacuum_respects_retention_skips_recent_files(self):
        """Verify files within retention period are not deleted."""
        table_location = "s3://main-bucket/table"
        external_path = "s3://external/recent.parquet"

        # File removed 1 hour ago (within 7-day retention)
        recent_timestamp = int((datetime.now().timestamp() - 1 * 60 * 60) * 1000)

        entries = [
            {
                "remove": {
                    "path": external_path,
                    "deletionTimestamp": recent_timestamp,
                },
                "_version": 1,
            },
        ]

        removes = parse_remove_actions(entries)
        adds = parse_add_actions(entries)

        orphaned = find_orphaned_external_paths(
            removes=removes,
            adds=adds,
            table_location=table_location,
            retention_hours=168,  # 7 days
        )

        assert len(orphaned) == 0

    @pytest.mark.standard
    def test_vacuum_respects_retention_deletes_old_files(self):
        """Verify files outside retention period are marked for deletion."""
        table_location = "s3://main-bucket/table"
        external_path = "s3://external/old.parquet"

        # File removed 10 days ago (outside 7-day retention)
        old_timestamp = int((datetime.now().timestamp() - 10 * 24 * 60 * 60) * 1000)

        entries = [
            {
                "remove": {
                    "path": external_path,
                    "deletionTimestamp": old_timestamp,
                },
                "_version": 1,
            },
        ]

        removes = parse_remove_actions(entries)
        adds = parse_add_actions(entries)

        orphaned = find_orphaned_external_paths(
            removes=removes,
            adds=adds,
            table_location=table_location,
            retention_hours=168,
        )

        assert len(orphaned) == 1
        assert orphaned[0] == external_path

    @pytest.mark.standard
    def test_vacuum_respects_custom_retention(self):
        """Verify custom retention period is honored."""
        table_location = "s3://main-bucket/table"
        external_path = "s3://external/medium.parquet"

        # File removed 2 days ago
        medium_timestamp = int((datetime.now().timestamp() - 2 * 24 * 60 * 60) * 1000)

        entries = [
            {
                "remove": {
                    "path": external_path,
                    "deletionTimestamp": medium_timestamp,
                },
                "_version": 1,
            },
        ]

        removes = parse_remove_actions(entries)
        adds = parse_add_actions(entries)

        # With 1-day retention, file should be orphaned
        orphaned_1day = find_orphaned_external_paths(
            removes=removes,
            adds=adds,
            table_location=table_location,
            retention_hours=24,  # 1 day
        )
        assert len(orphaned_1day) == 1

        # With 7-day retention, file should NOT be orphaned
        orphaned_7day = find_orphaned_external_paths(
            removes=removes,
            adds=adds,
            table_location=table_location,
            retention_hours=168,  # 7 days
        )
        assert len(orphaned_7day) == 0

    @pytest.mark.standard
    def test_vacuum_skips_internal_paths(self):
        """Verify files inside table root are skipped (handled by standard VACUUM)."""
        table_location = "s3://main-bucket/tables/my_table"

        old_timestamp = int((datetime.now().timestamp() - 10 * 24 * 60 * 60) * 1000)

        # File is inside the table location
        entries = [
            {
                "remove": {
                    "path": f"{table_location}/data/file.parquet",
                    "deletionTimestamp": old_timestamp,
                },
                "_version": 1,
            },
        ]

        removes = parse_remove_actions(entries)
        adds = parse_add_actions(entries)

        orphaned = find_orphaned_external_paths(
            removes=removes,
            adds=adds,
            table_location=table_location,
            retention_hours=168,
        )

        # Internal paths should be skipped
        assert len(orphaned) == 0
