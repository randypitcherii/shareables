"""Schema inference from Google Sheets data."""

from __future__ import annotations

import re
from typing import Any


def infer_column_type(values: list[Any]) -> str:
    """Infer Databricks SQL type from column values.

    Uses conservative inference - defaults to STRING unless
    values clearly match a specific type pattern.

    Args:
        values: List of string values from the column

    Returns:
        Databricks SQL type string (STRING, BIGINT, DOUBLE, BOOLEAN, DATE, TIMESTAMP)
    """
    # Filter out empty/None values
    non_empty = [v for v in values if v is not None and str(v).strip() != ""]

    if not non_empty:
        return "STRING"

    # Check each value against type patterns
    type_checks = {
        "BOOLEAN": _is_boolean,
        "TIMESTAMP": _is_timestamp,
        "DATE": _is_date,
        "BIGINT": _is_integer,
        "DOUBLE": _is_double,
    }

    for type_name, check_func in type_checks.items():
        if all(check_func(str(v)) for v in non_empty):
            return type_name

    return "STRING"


def _is_boolean(value: str) -> bool:
    """Check if value is a boolean."""
    return value.lower() in ("true", "false")


def _is_integer(value: str) -> bool:
    """Check if value is a clean integer (no leading zeros except for '0')."""
    if not value:
        return False
    # Reject leading zeros (could be ID, zip code, etc.)
    if len(value) > 1 and value[0] == "0" and value[1] != ".":
        return False
    try:
        int(value)
        return "." not in value and "e" not in value.lower()
    except ValueError:
        return False


def _is_double(value: str) -> bool:
    """Check if value is a decimal number."""
    if not value:
        return False
    # Reject leading zeros (could be ID, zip code, etc.)
    if len(value) > 1 and value[0] == "0" and value[1] != ".":
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False


def _is_date(value: str) -> bool:
    """Check if value matches ISO date format (YYYY-MM-DD)."""
    pattern = r"^\d{4}-\d{2}-\d{2}$"
    return bool(re.match(pattern, value))


def _is_timestamp(value: str) -> bool:
    """Check if value matches ISO timestamp format."""
    # YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS
    pattern = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
    return bool(re.match(pattern, value))


def sanitize_column_name(name: str) -> str:
    """Sanitize column name for SQL compatibility.

    Args:
        name: Original column name from sheet

    Returns:
        SQL-safe column name
    """
    # Lowercase
    result = name.lower()
    # Replace spaces, dashes, slashes with underscore
    result = re.sub(r"[\s\-/]+", "_", result)
    # Remove other special characters
    result = re.sub(r"[^a-z0-9_]", "", result)
    # Collapse multiple underscores
    result = re.sub(r"_+", "_", result)
    # Strip leading/trailing underscores
    result = result.strip("_")
    # Prefix with underscore if starts with number
    if result and result[0].isdigit():
        result = f"_{result}"
    # Fallback for empty result
    if not result:
        result = "column"
    return result


def sanitize_table_name(name: str) -> str:
    """Sanitize table/view name for SQL compatibility.

    Args:
        name: Original worksheet name

    Returns:
        SQL-safe table name
    """
    return sanitize_column_name(name)


def infer_schema(
    headers: list[str],
    rows: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """Infer full schema from headers and row data.

    Args:
        headers: List of column names from sheet
        rows: List of row dictionaries

    Returns:
        List of column definitions with 'name', 'sql_name', and 'type'
    """
    schema = []

    for header in headers:
        # Collect all values for this column
        values = [row.get(header) for row in rows]

        col_type = infer_column_type(values)
        sql_name = sanitize_column_name(header)

        schema.append({
            "name": header,
            "sql_name": sql_name,
            "type": col_type,
        })

    return schema
