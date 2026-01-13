"""Conversion evaluation results tracking and reporting.

Tracks outcomes of Glue-to-UC Delta conversion attempts across:
- 7 scenarios (standard, recursive, scattered, cross_bucket, cross_region, shared_a, shared_b)
- 3 methods (CONVERT TO DELTA, SHALLOW CLONE, Manual Delta Log)

Key metrics tracked:
- success: Did conversion work?
- requires_data_copy: Did method copy data? (DISQUALIFIER)
- reads_data_files: Did method read actual data? (cold storage unsafe)
"""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class ConversionResult:
    """Result of a single conversion attempt.

    Attributes:
        scenario: Base scenario name (e.g., "standard", "scattered")
        method: Conversion method ("convert", "clone", "manual")
        success: Whether conversion completed successfully
        error_message: Error details if failed
        requires_data_copy: Whether method copied data (DISQUALIFIER for recommendation)
        reads_data_files: Whether method read data files (unsafe for cold storage)
        source_table: Glue source table name
        target_table: UC Delta table name (if successful)
        row_count_before: Rows in source
        row_count_after: Rows in target (if successful)
        execution_time_ms: Time to execute conversion
        notes: Additional observations
    """
    scenario: str
    method: str
    success: bool = False
    error_message: str | None = None
    requires_data_copy: bool = False
    reads_data_files: bool = False
    source_table: str = ""
    target_table: str | None = None
    row_count_before: int = 0
    row_count_after: int | None = None
    execution_time_ms: int | None = None
    notes: str = ""

    @property
    def is_recommended(self) -> bool:
        """Check if this method is recommendable for this scenario.

        A method is recommended if:
        1. It succeeded
        2. It does NOT require data copies
        """
        return self.success and not self.requires_data_copy

    @property
    def is_cold_storage_safe(self) -> bool:
        """Check if method is safe for cold storage (Glacier).

        Safe if method does NOT read data files during conversion.
        """
        return not self.reads_data_files

    @property
    def status_emoji(self) -> str:
        """Get status emoji for display."""
        if not self.success:
            return "❌"
        if self.requires_data_copy:
            return "⚠️"  # Works but disqualified
        return "✅"


@dataclass
class PostConversionResult:
    """Result of testing a post-conversion UC Delta table operation.

    Attributes:
        scenario: Scenario name
        method: Conversion method that created the table
        operation: Operation tested (select_all, select_filtered, insert, update, delete, optimize, vacuum)
        success: Whether operation succeeded
        error_message: Error details if failed
        notes: Additional observations
    """
    scenario: str
    method: str
    operation: str
    success: bool = False
    error_message: str | None = None
    notes: str = ""


@dataclass
class ConversionMatrix:
    """Matrix tracking all conversion results.

    Maintains results for:
    - Conversion attempts (scenario × method)
    - Post-conversion operations (scenario × method × operation)
    """
    conversion_results: dict[str, dict[str, ConversionResult]] = field(default_factory=dict)
    post_conversion_results: dict[str, dict[str, dict[str, PostConversionResult]]] = field(default_factory=dict)
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def record_conversion(self, result: ConversionResult) -> None:
        """Record a conversion result.

        Args:
            result: ConversionResult to record
        """
        if result.scenario not in self.conversion_results:
            self.conversion_results[result.scenario] = {}
        self.conversion_results[result.scenario][result.method] = result

    def record_post_conversion(self, result: PostConversionResult) -> None:
        """Record a post-conversion operation result.

        Args:
            result: PostConversionResult to record
        """
        if result.scenario not in self.post_conversion_results:
            self.post_conversion_results[result.scenario] = {}
        if result.method not in self.post_conversion_results[result.scenario]:
            self.post_conversion_results[result.scenario][result.method] = {}
        self.post_conversion_results[result.scenario][result.method][result.operation] = result

    def get_conversion_result(self, scenario: str, method: str) -> ConversionResult | None:
        """Get conversion result for scenario/method."""
        return self.conversion_results.get(scenario, {}).get(method)

    def get_successful_conversions(self) -> list[ConversionResult]:
        """Get all successful conversion results."""
        results = []
        for scenario_results in self.conversion_results.values():
            for result in scenario_results.values():
                if result.success:
                    results.append(result)
        return results

    def get_recommended_methods(self, scenario: str) -> list[str]:
        """Get recommended conversion methods for a scenario.

        A method is recommended if it works AND doesn't require data copy.

        Args:
            scenario: Scenario name

        Returns:
            List of recommended method names
        """
        recommended = []
        for method, result in self.conversion_results.get(scenario, {}).items():
            if result.is_recommended:
                recommended.append(method)
        return recommended

    def to_dict(self) -> dict[str, Any]:
        """Convert matrix to dictionary for serialization."""
        return {
            "generated_at": self.generated_at,
            "conversion_results": {
                scenario: {method: asdict(result) for method, result in methods.items()}
                for scenario, methods in self.conversion_results.items()
            },
            "post_conversion_results": {
                scenario: {
                    method: {op: asdict(result) for op, result in ops.items()}
                    for method, ops in methods.items()
                }
                for scenario, methods in self.post_conversion_results.items()
            },
        }

    def save_json(self, path: str | Path) -> None:
        """Save matrix to JSON file."""
        path = Path(path)
        path.write_text(json.dumps(self.to_dict(), indent=2))
        print(f"✓ Saved conversion matrix to {path}")

    @classmethod
    def load_json(cls, path: str | Path) -> "ConversionMatrix":
        """Load matrix from JSON file."""
        path = Path(path)
        data = json.loads(path.read_text())

        matrix = cls()
        matrix.generated_at = data.get("generated_at", "")

        for scenario, methods in data.get("conversion_results", {}).items():
            for method, result_dict in methods.items():
                result = ConversionResult(**result_dict)
                matrix.record_conversion(result)

        for scenario, methods in data.get("post_conversion_results", {}).items():
            for method, ops in methods.items():
                for op, result_dict in ops.items():
                    result = PostConversionResult(**result_dict)
                    matrix.record_post_conversion(result)

        return matrix

    def generate_markdown_table(self) -> str:
        """Generate markdown table of conversion results."""
        lines = [
            "# Conversion Matrix Results",
            "",
            f"Generated: {self.generated_at}",
            "",
            "## Legend",
            "- ✅ = Works, no data copy (RECOMMENDED)",
            "- ⚠️ = Works, but requires data copy (NOT RECOMMENDED)",
            "- ❌ = Failed",
            "- 🧊 = Cold storage safe (doesn't read data files)",
            "",
            "## Conversion Results",
            "",
            "| Scenario | Method | Status | Data Copy | File Reads | Cold Safe | Recommended |",
            "|----------|--------|--------|-----------|------------|-----------|-------------|",
        ]

        scenarios = ["standard", "recursive", "scattered", "cross_bucket", "cross_region", "shared_a", "shared_b"]
        methods = ["convert", "clone", "manual"]

        for scenario in scenarios:
            for method in methods:
                result = self.get_conversion_result(scenario, method)
                if result:
                    status = result.status_emoji
                    data_copy = "Yes" if result.requires_data_copy else "No"
                    file_reads = "Yes" if result.reads_data_files else "No"
                    cold_safe = "🧊" if result.is_cold_storage_safe else "❌"
                    recommended = "✅" if result.is_recommended else "❌"
                else:
                    status = "?"
                    data_copy = "?"
                    file_reads = "?"
                    cold_safe = "?"
                    recommended = "?"

                lines.append(f"| {scenario} | {method} | {status} | {data_copy} | {file_reads} | {cold_safe} | {recommended} |")

        return "\n".join(lines)

    def save_markdown(self, path: str | Path) -> None:
        """Save matrix as markdown file."""
        path = Path(path)
        content = self.generate_markdown_table()
        path.write_text(content)
        print(f"✓ Saved conversion matrix markdown to {path}")


# Singleton instance for test result collection
_conversion_matrix: ConversionMatrix | None = None


def get_conversion_matrix() -> ConversionMatrix:
    """Get the global conversion matrix instance."""
    global _conversion_matrix
    if _conversion_matrix is None:
        _conversion_matrix = ConversionMatrix()
    return _conversion_matrix


def reset_conversion_matrix() -> None:
    """Reset the global conversion matrix."""
    global _conversion_matrix
    _conversion_matrix = ConversionMatrix()


def record_conversion_result(
    scenario: str,
    method: str,
    success: bool,
    error_message: str | None = None,
    requires_data_copy: bool = False,
    reads_data_files: bool = False,
    source_table: str = "",
    target_table: str | None = None,
    row_count_before: int = 0,
    row_count_after: int | None = None,
    execution_time_ms: int | None = None,
    notes: str = "",
) -> ConversionResult:
    """Record a conversion result to the global matrix.

    Convenience function for recording results during testing.
    """
    result = ConversionResult(
        scenario=scenario,
        method=method,
        success=success,
        error_message=error_message,
        requires_data_copy=requires_data_copy,
        reads_data_files=reads_data_files,
        source_table=source_table,
        target_table=target_table,
        row_count_before=row_count_before,
        row_count_after=row_count_after,
        execution_time_ms=execution_time_ms,
        notes=notes,
    )
    get_conversion_matrix().record_conversion(result)
    return result


def record_post_conversion_result(
    scenario: str,
    method: str,
    operation: str,
    success: bool,
    error_message: str | None = None,
    notes: str = "",
) -> PostConversionResult:
    """Record a post-conversion operation result to the global matrix."""
    result = PostConversionResult(
        scenario=scenario,
        method=method,
        operation=operation,
        success=success,
        error_message=error_message,
        notes=notes,
    )
    get_conversion_matrix().record_post_conversion(result)
    return result
