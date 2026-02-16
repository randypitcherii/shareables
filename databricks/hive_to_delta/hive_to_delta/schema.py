"""Schema inference for Hive to Delta table conversion.

Provides two paths for building Delta-compatible schema dictionaries:
- Glue-based: from AWS Glue column definitions (existing)
- Spark-based: from a Spark StructType (new)
"""

from typing import Any


# Mapping of AWS Glue/Hive types to Delta Lake types
GLUE_TO_DELTA_TYPE_MAP: dict[str, str] = {
    # String types
    "string": "string",
    "char": "string",
    "varchar": "string",
    # Numeric types
    "tinyint": "byte",
    "smallint": "short",
    "int": "integer",
    "integer": "integer",
    "bigint": "long",
    "float": "float",
    "double": "double",
    "decimal": "decimal",
    # Boolean
    "boolean": "boolean",
    # Date/Time types
    "date": "date",
    "timestamp": "timestamp",
    # Binary
    "binary": "binary",
    # Complex types (simplified mapping)
    "array": "array",
    "map": "map",
    "struct": "struct",
}


def _normalize_glue_type(glue_type: str) -> str:
    """Normalize Glue type string for mapping.

    Handles parameterized types like decimal(10,2), varchar(255), etc.
    """
    glue_type = glue_type.lower().strip()

    # Handle parameterized types - extract base type
    if "(" in glue_type:
        base_type = glue_type.split("(")[0]
        # For decimal, preserve the full type specification
        if base_type == "decimal":
            return glue_type  # Return full decimal(p,s)
        return base_type

    return glue_type


def _map_glue_to_delta_type(glue_type: str) -> str:
    """Map a single Glue type to Delta type."""
    normalized = _normalize_glue_type(glue_type)

    # Handle decimal with precision/scale
    if normalized.startswith("decimal"):
        return normalized  # Keep as decimal(p,s)

    return GLUE_TO_DELTA_TYPE_MAP.get(normalized, "string")


def build_delta_schema_from_glue(glue_columns: list[dict[str, str]]) -> dict[str, Any]:
    """Build Delta schema from Glue column definitions.

    Args:
        glue_columns: List of column dicts with 'Name' and 'Type' keys
                     (as returned by Glue API)

    Returns:
        Delta schema dict with 'type': 'struct' and 'fields' list

    Raises:
        ValueError: If no columns provided or if any column has empty name
    """
    if not glue_columns:
        raise ValueError("Invalid Glue schema: no columns provided")

    fields = []

    for col in glue_columns:
        col_name = col.get("Name", col.get("name", ""))
        if not col_name or col_name.strip() == "":
            raise ValueError(f"Invalid Glue schema: column without name found in schema: {col}")

        col_type = col.get("Type", col.get("type", "string"))

        delta_type = _map_glue_to_delta_type(col_type)

        fields.append({
            "name": col_name,
            "type": delta_type,
            "nullable": True,
            "metadata": {},
        })

    return {
        "type": "struct",
        "fields": fields,
    }


def build_delta_schema_from_spark(spark_schema: Any) -> dict[str, Any]:
    """Build Delta schema from a Spark StructType.

    Args:
        spark_schema: A Spark StructType containing StructFields

    Returns:
        Delta schema dict with 'type': 'struct' and 'fields' list

    Raises:
        ValueError: If the schema has no fields
    """
    if not spark_schema.fields:
        raise ValueError("Invalid Spark schema: no fields provided")

    fields = []

    for spark_field in spark_schema.fields:
        fields.append({
            "name": spark_field.name,
            "type": spark_field.dataType.simpleString(),
            "nullable": spark_field.nullable,
            "metadata": {},
        })

    return {
        "type": "struct",
        "fields": fields,
    }
