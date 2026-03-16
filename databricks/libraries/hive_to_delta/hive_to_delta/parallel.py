"""Parallel execution utilities for hive_to_delta."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class ConversionSummary:
    """Summary statistics for a bulk conversion operation.

    Aggregates results from multiple table conversions including
    success/failure counts, timing, and file statistics.
    """

    total_tables: int
    succeeded: int
    failed: int
    total_files: int
    total_duration_seconds: float
    failed_tables: list[tuple[str, str]]  # List of (table_name, error_message)

    @property
    def success_rate(self) -> float:
        """Calculate success rate as a percentage."""
        if self.total_tables == 0:
            return 0.0
        return (self.succeeded / self.total_tables) * 100

    def __str__(self) -> str:
        """Format summary for display."""
        lines = [
            "\n" + "=" * 60,
            "CONVERSION SUMMARY",
            "=" * 60,
            f"Total tables: {self.total_tables}",
            f"Succeeded: {self.succeeded}",
            f"Failed: {self.failed}",
            f"Success rate: {self.success_rate:.1f}%",
            f"Total files processed: {self.total_files}",
            f"Total duration: {self.total_duration_seconds:.2f}s",
        ]

        if self.failed_tables:
            lines.extend([
                "\nFailed tables:",
            ])
            for table_name, error in self.failed_tables:
                lines.append(f"  - {table_name}: {error}")

        lines.append("=" * 60)
        return "\n".join(lines)


def print_progress(completed: int, total: int, current_item: str) -> None:
    """Print progress to stdout.

    Args:
        completed: Number of items completed.
        total: Total number of items.
        current_item: Name/identifier of the current item being processed.
    """
    print(f"[{completed}/{total}] Completed: {current_item}")


def run_parallel(
    func: Callable[[T], R],
    items: list[T],
    max_workers: int = 4,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> list[R | Exception]:
    """Run a function in parallel across items using ThreadPoolExecutor.

    Uses threads because the work is I/O bound (S3/Glue API calls).
    Each worker should create its own boto3 clients for thread safety.

    Args:
        func: Function to call for each item. Signature: func(item) -> result
        items: List of items to process.
        max_workers: Maximum number of concurrent workers.
        progress_callback: Optional callback for progress updates.
            Signature: callback(completed, total, current_item_description)
            If not provided, uses default print_progress().

    Returns:
        List of results in completion order. Failed items return the Exception
        instead of the result, allowing the caller to handle errors.

    Example:
        def process_table(table_name):
            # ... conversion logic
            return ConversionResult(...)

        # With default progress reporting
        results = run_parallel(process_table, ["table1", "table2"], max_workers=4)

        # With custom progress callback
        def custom_progress(completed, total, item):
            logger.info(f"Progress: {completed}/{total} - {item}")

        results = run_parallel(
            process_table,
            ["table1", "table2"],
            max_workers=4,
            progress_callback=custom_progress
        )

        for result in results:
            if isinstance(result, Exception):
                print(f"Error: {result}")
            else:
                print(f"Success: {result}")
    """
    if not items:
        return []

    results: list[R | Exception] = []
    total = len(items)
    completed = 0
    callback = progress_callback or print_progress

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and track which item each future corresponds to
        future_to_item = {executor.submit(func, item): item for item in items}

        # Collect results as they complete
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            completed += 1

            try:
                result = future.result()
                results.append(result)
                callback(completed, total, str(item))
            except Exception as e:
                # Don't fail the entire batch - return the exception
                results.append(e)
                callback(completed, total, f"{item} (FAILED: {e})")

    return results


def create_summary(results: list[Any]) -> ConversionSummary:
    """Create a ConversionSummary from a list of ConversionResult objects.

    Args:
        results: List of ConversionResult objects (and possibly Exceptions)

    Returns:
        ConversionSummary with aggregated statistics

    Example:
        >>> results = convert_tables(spark, "db", ["t1", "t2"], "cat", "schema")
        >>> summary = create_summary(results)
        >>> print(summary)
        >>> print(f"Success rate: {summary.success_rate:.1f}%")
    """
    from hive_to_delta.models import ConversionResult

    total_tables = len(results)
    succeeded = 0
    failed = 0
    total_files = 0
    total_duration = 0.0
    failed_tables: list[tuple[str, str]] = []

    for result in results:
        if isinstance(result, Exception):
            # Exception was raised and captured
            failed += 1
            failed_tables.append(("unknown", str(result)))
        elif isinstance(result, ConversionResult):
            # ConversionResult object
            if result.success:
                succeeded += 1
                total_files += result.file_count
            else:
                failed += 1
                failed_tables.append((result.source_table, result.error or "Unknown error"))

            total_duration += result.duration_seconds
        else:
            # Unexpected result type
            failed += 1
            failed_tables.append(("unknown", f"Unexpected result type: {type(result)}"))

    return ConversionSummary(
        total_tables=total_tables,
        succeeded=succeeded,
        failed=failed,
        total_files=total_files,
        total_duration_seconds=total_duration,
        failed_tables=failed_tables,
    )
