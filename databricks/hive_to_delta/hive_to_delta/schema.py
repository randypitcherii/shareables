"""Schema inference for Hive to Delta table conversion.

Provides two paths for building Delta-compatible schema dictionaries:
- Glue-based: from AWS Glue column definitions (existing)
- Spark-based: from a Spark StructType (new)

Complex types (array, map, struct) are converted to Delta protocol JSON
objects as required by the Delta transaction log specification. Simple types
remain as strings (e.g., "string", "long", "integer").
"""

from typing import Any, Union


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
    # Complex types (bare names only — parameterized complex types are
    # parsed by _parse_hive_type instead)
    "array": "array",
    "map": "map",
    "struct": "struct",
}

# Delta protocol JSON type representation
DeltaType = Union[str, dict[str, Any]]


# =============================================================================
# Hive type string parser
# =============================================================================
#
# Recursive descent parser for Hive/Glue type notation. Converts type strings
# like "array<struct<id:int,tags:array<string>>>" into Delta protocol JSON
# objects. Pure Python — no Spark session required.
#
# Grammar:
#   type        := array_type | map_type | struct_type | decimal_type | simple_type
#   array_type  := "array" "<" type ">"
#   map_type    := "map" "<" type "," type ">"
#   struct_type := "struct" "<" field ("," field)* ">"
#   field       := name ":" type
#   decimal_type:= "decimal" "(" digits "," digits ")"
#   simple_type := identifier (mapped via GLUE_TO_DELTA_TYPE_MAP)


def _split_at_top_level(s: str, delimiter: str) -> list[str]:
    """Split a string by delimiter, but only at the top level (outside angle brackets).

    Handles nested angle brackets so that e.g. "string,array<int>" splits
    correctly as ["string", "array<int>"] instead of splitting inside the <>.
    """
    parts = []
    depth = 0
    current = []

    for char in s:
        if char == "<":
            depth += 1
            current.append(char)
        elif char == ">":
            depth -= 1
            current.append(char)
        elif char == delimiter and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)

    if current:
        parts.append("".join(current).strip())

    return parts


def _parse_hive_type(type_str: str) -> DeltaType:
    """Parse a Hive type string into Delta protocol JSON format.

    Pure Python recursive descent parser — no Spark session required.

    Args:
        type_str: A Hive type string like "array<string>", "map<string,int>",
                 "struct<name:string,age:int>", or a simple type like "bigint".

    Returns:
        Delta protocol type representation:
        - Simple types: string (e.g., "string", "long", "integer")
        - Array: {"type": "array", "elementType": <type>, "containsNull": True}
        - Map: {"type": "map", "keyType": <type>, "valueType": <type>, "valueContainsNull": True}
        - Struct: {"type": "struct", "fields": [{"name": ..., "type": ..., ...}, ...]}

    Examples:
        >>> _parse_hive_type("array<string>")
        {"type": "array", "elementType": "string", "containsNull": True}
        >>> _parse_hive_type("map<string,int>")
        {"type": "map", "keyType": "string", "valueType": "integer", "valueContainsNull": True}
        >>> _parse_hive_type("struct<name:string,age:int>")
        {"type": "struct", "fields": [...]}
    """
    type_str = type_str.strip().lower()

    # Extract base type and parameters (content inside angle brackets)
    bracket_pos = type_str.find("<")
    if bracket_pos == -1:
        # Simple type or decimal with parens
        return _map_simple_type(type_str)

    base = type_str[:bracket_pos].strip()
    # Extract content between outermost < and >
    inner = type_str[bracket_pos + 1:-1].strip()

    if base == "array":
        element_type = _parse_hive_type(inner)
        return {
            "type": "array",
            "elementType": element_type,
            "containsNull": True,
        }

    elif base == "map":
        parts = _split_at_top_level(inner, ",")
        if len(parts) != 2:
            raise ValueError(f"Invalid map type: expected 2 type parameters, got {len(parts)}: {type_str}")
        key_type = _parse_hive_type(parts[0])
        value_type = _parse_hive_type(parts[1])
        return {
            "type": "map",
            "keyType": key_type,
            "valueType": value_type,
            "valueContainsNull": True,
        }

    elif base == "struct":
        fields = []
        field_strs = _split_at_top_level(inner, ",")
        for field_str in field_strs:
            colon_pos = field_str.find(":")
            if colon_pos == -1:
                raise ValueError(f"Invalid struct field (missing ':'): {field_str}")
            field_name = field_str[:colon_pos].strip()
            field_type = _parse_hive_type(field_str[colon_pos + 1:])
            fields.append({
                "name": field_name,
                "type": field_type,
                "nullable": True,
                "metadata": {},
            })
        return {
            "type": "struct",
            "fields": fields,
        }

    else:
        raise ValueError(f"Unknown parameterized type: {base}")


def _map_simple_type(type_str: str) -> str:
    """Map a simple (non-parameterized) Hive type to Delta type string.

    Handles decimal(p,s) and simple type names via GLUE_TO_DELTA_TYPE_MAP.
    """
    type_str = type_str.strip().lower()

    # Handle decimal with precision/scale
    if type_str.startswith("decimal"):
        return type_str  # Keep as decimal(p,s)

    # Handle parameterized char/varchar — extract base type
    if "(" in type_str:
        base = type_str.split("(")[0]
        return GLUE_TO_DELTA_TYPE_MAP.get(base, "string")

    return GLUE_TO_DELTA_TYPE_MAP.get(type_str, "string")


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


def _map_glue_to_delta_type(glue_type: str) -> DeltaType:
    """Map a single Glue type to Delta type.

    Handles three categories:
    - Simple types (string, int, etc.): mapped via GLUE_TO_DELTA_TYPE_MAP
    - Decimal with params (decimal(10,2)): preserved as-is
    - Complex types with angle brackets (array<string>, map<string,int>,
      struct<name:string,age:int>): parsed into Delta protocol JSON objects
    """
    normalized = _normalize_glue_type(glue_type)

    # Handle decimal with precision/scale
    if normalized.startswith("decimal"):
        return normalized  # Keep as decimal(p,s)

    # Handle parameterized complex types (array<string>, map<k,v>, struct<...>)
    # Parse into Delta protocol JSON objects
    if "<" in normalized:
        return _parse_hive_type(normalized)

    return GLUE_TO_DELTA_TYPE_MAP.get(normalized, "string")


def build_delta_schema_from_glue(glue_columns: list[dict[str, str]]) -> dict[str, Any]:
    """Build Delta schema from Glue column definitions.

    Args:
        glue_columns: List of column dicts with 'Name' and 'Type' keys
                     (as returned by Glue API)

    Returns:
        Delta schema dict with 'type': 'struct' and 'fields' list.
        Complex types use Delta protocol JSON objects.

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

    Uses jsonValue() on each field's dataType, which returns:
    - Simple types: string names ("string", "long", "integer", etc.)
    - Complex types: Delta protocol JSON objects ({"type": "array", ...})

    Args:
        spark_schema: A Spark StructType containing StructFields

    Returns:
        Delta schema dict with 'type': 'struct' and 'fields' list.
        Complex types use Delta protocol JSON objects.

    Raises:
        ValueError: If the schema has no fields
    """
    if not spark_schema.fields:
        raise ValueError("Invalid Spark schema: no fields provided")

    fields = []

    for spark_field in spark_schema.fields:
        delta_type = spark_field.dataType.jsonValue()

        fields.append({
            "name": spark_field.name,
            "type": delta_type,
            "nullable": spark_field.nullable,
            "metadata": {},
        })

    return {
        "type": "struct",
        "fields": fields,
    }
