"""Data models for hive_to_delta package."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ParquetFileInfo:
    """Information about a parquet file discovered in a partition."""

    path: str  # Full S3 path (absolute)
    size: int  # File size in bytes
    partition_values: dict[str, str] = field(default_factory=dict)

    @property
    def is_absolute_path(self) -> bool:
        """Check if path is absolute (starts with s3://)."""
        return self.path.startswith("s3://")


@dataclass
class ConversionResult:
    """Result of converting a single Hive table to Delta."""

    source_table: str  # Glue table name
    target_table: str  # UC fully-qualified name (catalog.schema.table)
    success: bool
    file_count: int = 0
    delta_log_location: str = ""
    error: Optional[str] = None
    duration_seconds: float = 0.0

    def __str__(self) -> str:
        status = "✓" if self.success else f"✗ {self.error}"
        return f"{self.source_table} -> {self.target_table}: {status}"


@dataclass
class VacuumResult:
    """Result of vacuuming external files for a Delta table."""

    table_name: str
    orphaned_files: list[str] = field(default_factory=list)
    deleted_files: list[str] = field(default_factory=list)
    dry_run: bool = True
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None

    def __str__(self) -> str:
        if self.dry_run:
            return f"{self.table_name}: {len(self.orphaned_files)} orphaned files (dry run)"
        return f"{self.table_name}: {len(self.deleted_files)} files deleted"


@dataclass
class TableInfo:
    """Metadata about a table to be converted.

    Produced by Discovery strategies, consumed by the conversion pipeline.
    """

    name: str  # Table name
    location: str  # S3 root path for the table data
    target_table_name: Optional[str] = None  # Override for UC table name
    columns: Optional[list[dict[str, str]]] = None  # Glue-style column defs (if available)
    partition_keys: list[str] = field(default_factory=list)
