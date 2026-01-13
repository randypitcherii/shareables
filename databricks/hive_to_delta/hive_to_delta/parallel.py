"""Parallel execution utilities for hive_to_delta."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, TypeVar

T = TypeVar("T")
R = TypeVar("R")


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
) -> list[R | Exception]:
    """Run a function in parallel across items using ThreadPoolExecutor.

    Uses threads because the work is I/O bound (S3/Glue API calls).
    Each worker should create its own boto3 clients for thread safety.

    Args:
        func: Function to call for each item. Signature: func(item) -> result
        items: List of items to process.
        max_workers: Maximum number of concurrent workers.

    Returns:
        List of results in completion order. Failed items return the Exception
        instead of the result, allowing the caller to handle errors.

    Example:
        def process_table(table_name):
            # ... conversion logic
            return ConversionResult(...)

        results = run_parallel(process_table, ["table1", "table2"], max_workers=4)
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
                print_progress(completed, total, str(item))
            except Exception as e:
                # Don't fail the entire batch - return the exception
                results.append(e)
                print_progress(completed, total, f"{item} (FAILED: {e})")

    return results
