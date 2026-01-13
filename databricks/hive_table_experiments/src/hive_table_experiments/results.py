"""Results matrix tracking and reporting for Hive table experiments.

Provides structured collection and reporting of test results across:
- Multiple access methods (glue_hive, glue_fed, uc_ext, uc_delta)
- Multiple scenarios (standard, recursive, scattered, etc.)
- Multiple operations (READ, INSERT, UPDATE, DELETE, OPTIMIZE, VACUUM)
- Migration metrics (data_copy, file_read, cold_storage)
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class OperationResult(Enum):
    """Result status for a test operation."""

    OK = "OK"  # Operation succeeded as expected
    FAIL = "FAIL"  # Operation failed (may be expected or unexpected)
    PARTIAL = "PARTIAL"  # Partially works (e.g., some rows visible)
    NOT_SUPPORTED = "N/A"  # Operation not supported for this method/format
    UNTESTED = "?"  # Not yet tested
    NEEDS_CONSOLIDATE = "CONSOLIDATE"  # Requires data consolidation first
    ZERO_ROWS = "0 ROWS"  # Query succeeded but returned no data


class Operation(Enum):
    """Operations to test for each scenario/method combination."""

    SCHEMA_READ = "schema_read"
    PARTITION_DISCOVERY = "partition_discovery"
    SELECT_ALL = "select_all"
    SELECT_FILTERED = "select_filtered"
    INSERT = "insert"
    INSERT_OVERWRITE = "insert_overwrite"
    UPDATE = "update"
    DELETE = "delete"
    OPTIMIZE = "optimize"
    VACUUM = "vacuum"


@dataclass
class ScenarioResult:
    """Results for one scenario/access-method/operation combination.

    Attributes:
        scenario: Scenario name (e.g., "standard", "scattered")
        access_method: Access method suffix (e.g., "glue_hive", "uc_delta")
        operation: Operation tested (e.g., "select_all", "insert")
        result: OperationResult enum value
        row_count: Number of rows returned (for queries)
        latency_ms: Query latency in milliseconds
        error: Error message if operation failed
        notes: Additional notes about the result
        timestamp: When the test was run
    """

    scenario: str
    access_method: str
    operation: str
    result: OperationResult = OperationResult.UNTESTED
    row_count: Optional[int] = None
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    notes: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for DataFrame creation."""
        return {
            "scenario": self.scenario,
            "access_method": self.access_method,
            "operation": self.operation,
            "result": self.result.value if isinstance(self.result, OperationResult) else self.result,
            "row_count": self.row_count,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "notes": self.notes,
            "timestamp": self.timestamp,
        }


@dataclass
class MigrationMetrics:
    """Migration metrics for a scenario/access-method combination.

    Tracks whether the migration approach requires:
    - Data copying (expensive for large tables)
    - Reading data files (problematic for cold storage)
    - Whether it works with S3 Glacier/cold storage
    """

    scenario: str
    access_method: str
    data_copy_required: bool = False
    file_read_required: bool = False
    cold_storage_friendly: bool = True
    metadata_only: bool = False
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for DataFrame creation."""
        return {
            "scenario": self.scenario,
            "access_method": self.access_method,
            "data_copy": "Yes" if self.data_copy_required else "No",
            "file_read": "Yes" if self.file_read_required else "No",
            "cold_storage_ok": "Yes" if self.cold_storage_friendly else "No",
            "metadata_only": "Yes" if self.metadata_only else "No",
            "notes": self.notes,
        }


class ResultsMatrix:
    """Collection of all test results with aggregation and reporting.

    Example usage:
        matrix = ResultsMatrix()
        matrix.record_result(
            scenario="standard",
            access_method="glue_hive",
            operation="select_all",
            result=OperationResult.OK,
            row_count=20,
            latency_ms=150,
        )
        matrix.record_migration_metrics(
            scenario="standard",
            access_method="glue_hive",
            data_copy_required=False,
            file_read_required=False,
        )
        print(matrix.to_markdown())
    """

    def __init__(self):
        self.results: list[ScenarioResult] = []
        self.migration_metrics: list[MigrationMetrics] = []
        self.start_time = datetime.now()

    def record_result(
        self,
        scenario: str,
        access_method: str,
        operation: str,
        result: OperationResult,
        row_count: Optional[int] = None,
        latency_ms: Optional[float] = None,
        error: Optional[str] = None,
        notes: str = "",
    ) -> None:
        """Record a single test result.

        Args:
            scenario: Scenario name (e.g., "standard")
            access_method: Access method (e.g., "glue_hive")
            operation: Operation name (e.g., "select_all")
            result: OperationResult enum value
            row_count: Number of rows returned
            latency_ms: Query latency in milliseconds
            error: Error message if failed
            notes: Additional notes
        """
        self.results.append(
            ScenarioResult(
                scenario=scenario,
                access_method=access_method,
                operation=operation,
                result=result,
                row_count=row_count,
                latency_ms=latency_ms,
                error=error,
                notes=notes,
            )
        )

    def record_migration_metrics(
        self,
        scenario: str,
        access_method: str,
        data_copy_required: bool = False,
        file_read_required: bool = False,
        cold_storage_friendly: bool = True,
        metadata_only: bool = False,
        notes: str = "",
    ) -> None:
        """Record migration metrics for a scenario/method combination.

        Args:
            scenario: Scenario name
            access_method: Access method
            data_copy_required: Whether data must be copied
            file_read_required: Whether setup reads data files
            cold_storage_friendly: Works with S3 Glacier
            metadata_only: Can be done via metadata operations only
            notes: Additional notes
        """
        self.migration_metrics.append(
            MigrationMetrics(
                scenario=scenario,
                access_method=access_method,
                data_copy_required=data_copy_required,
                file_read_required=file_read_required,
                cold_storage_friendly=cold_storage_friendly,
                metadata_only=metadata_only,
                notes=notes,
            )
        )

    def to_dataframe(self) -> "pd.DataFrame":
        """Convert results to pandas DataFrame.

        Returns:
            DataFrame with all test results

        Raises:
            ImportError: If pandas is not available
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for DataFrame export")

        return pd.DataFrame([r.to_dict() for r in self.results])

    def migration_metrics_dataframe(self) -> "pd.DataFrame":
        """Convert migration metrics to pandas DataFrame.

        Returns:
            DataFrame with migration metrics
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for DataFrame export")

        return pd.DataFrame([m.to_dict() for m in self.migration_metrics])

    def summary_pivot(self, operation: Optional[str] = None) -> "pd.DataFrame":
        """Generate scenario x access_method pivot table.

        Args:
            operation: Filter to specific operation (default: all)

        Returns:
            Pivot table with scenarios as rows, access_methods as columns
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for pivot table")

        df = self.to_dataframe()

        if operation:
            df = df[df["operation"] == operation]

        pivot = df.pivot_table(
            index="scenario",
            columns="access_method",
            values="result",
            aggfunc="first",  # Take first result if multiple
        )

        return pivot

    def to_markdown(self) -> str:
        """Generate markdown table of results.

        Returns:
            Markdown-formatted results matrix
        """
        lines = []
        lines.append("# Hive Table Experiments Results Matrix")
        lines.append("")
        lines.append(f"Generated: {datetime.now().isoformat()}")
        lines.append("")

        # Group by operation for cleaner output
        operations = sorted(set(r.operation for r in self.results))
        scenarios = sorted(set(r.scenario for r in self.results))
        methods = sorted(set(r.access_method for r in self.results))

        if not operations:
            lines.append("*No results recorded yet*")
            return "\n".join(lines)

        # Operations matrix
        lines.append("## Operations Matrix")
        lines.append("")
        header = "| Scenario | " + " | ".join(methods) + " |"
        separator = "|" + "|".join(["---"] * (len(methods) + 1)) + "|"
        lines.append(header)
        lines.append(separator)

        for op in operations:
            lines.append(f"### {op}")
            lines.append("")
            lines.append(header)
            lines.append(separator)

            for scenario in scenarios:
                row = [scenario]
                for method in methods:
                    result = self._get_result(scenario, method, op)
                    row.append(result)
                lines.append("| " + " | ".join(row) + " |")
            lines.append("")

        # Migration metrics
        if self.migration_metrics:
            lines.append("## Migration Metrics")
            lines.append("")
            lines.append("| Scenario | Method | Data Copy | File Read | Cold Storage OK |")
            lines.append("|---|---|---|---|---|")

            for m in self.migration_metrics:
                lines.append(
                    f"| {m.scenario} | {m.access_method} | "
                    f"{'Yes' if m.data_copy_required else 'No'} | "
                    f"{'Yes' if m.file_read_required else 'No'} | "
                    f"{'Yes' if m.cold_storage_friendly else 'No'} |"
                )

        return "\n".join(lines)

    def _get_result(self, scenario: str, access_method: str, operation: str) -> str:
        """Get result value for a specific combination."""
        for r in self.results:
            if r.scenario == scenario and r.access_method == access_method and r.operation == operation:
                if isinstance(r.result, OperationResult):
                    return r.result.value
                return str(r.result)
        return "?"

    def save_markdown(self, filepath: str) -> None:
        """Save results matrix to markdown file.

        Args:
            filepath: Path to save the markdown file
        """
        with open(filepath, "w") as f:
            f.write(self.to_markdown())

    def save_csv(self, filepath: str) -> None:
        """Save detailed results to CSV file.

        Args:
            filepath: Path to save the CSV file
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for CSV export")

        self.to_dataframe().to_csv(filepath, index=False)


# Global instance for easy access in notebooks
_global_matrix: Optional[ResultsMatrix] = None


def get_results_matrix() -> ResultsMatrix:
    """Get or create global results matrix instance.

    Returns:
        Global ResultsMatrix instance
    """
    global _global_matrix
    if _global_matrix is None:
        _global_matrix = ResultsMatrix()
    return _global_matrix


def reset_results_matrix() -> ResultsMatrix:
    """Reset global results matrix.

    Returns:
        New empty ResultsMatrix instance
    """
    global _global_matrix
    _global_matrix = ResultsMatrix()
    return _global_matrix
