"""Unit tests for data models."""

import pytest
from hive_to_delta.models import TableInfo


class TestTableInfo:
    """Tests for TableInfo dataclass."""

    def test_minimal_construction(self):
        """Test TableInfo with only required fields."""
        info = TableInfo(name="events", location="s3://bucket/events")
        assert info.name == "events"
        assert info.location == "s3://bucket/events"
        assert info.target_table_name is None
        assert info.columns is None
        assert info.partition_keys == []

    def test_full_construction(self):
        """Test TableInfo with all fields."""
        columns = [{"Name": "id", "Type": "bigint"}]
        info = TableInfo(
            name="events",
            location="s3://bucket/events",
            target_table_name="raw_events",
            columns=columns,
            partition_keys=["year", "month"],
        )
        assert info.target_table_name == "raw_events"
        assert info.columns == columns
        assert info.partition_keys == ["year", "month"]

    def test_partition_keys_default_empty(self):
        """Test that partition_keys defaults to empty list."""
        info = TableInfo(name="t", location="s3://b/t")
        assert info.partition_keys == []
        # Verify independent default (not shared mutable)
        info2 = TableInfo(name="t2", location="s3://b/t2")
        info.partition_keys.append("year")
        assert info2.partition_keys == []
