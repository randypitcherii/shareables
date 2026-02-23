# Complex Data Type Support for Hive-to-Delta Conversion

Implemented in PR #17.

## Problem

Two bugs prevented correct handling of complex data types (arrays, maps, structs):

1. **Glue path silent corruption**: Parameterized complex types from AWS Glue (e.g., `array<string>`, `map<string,int>`) were silently mapped to `"string"` because `_map_glue_to_delta_type()` didn't handle angle brackets.

2. **Non-compliant schema format**: Flat string representations (`"array<string>"`) don't satisfy the Delta protocol. Databricks rejects them at read time with `INVALID_JSON_DATA_TYPE`. Delta requires nested JSON objects:
   - Array: `{"type": "array", "elementType": "string", "containsNull": true}`
   - Map: `{"type": "map", "keyType": "string", "valueType": "integer", "valueContainsNull": true}`
   - Struct: `{"type": "struct", "fields": [...]}`

## Cluster Validation

Tested on `fe-vm-fe-randy-pitcher-workspace.cloud.databricks.com`:

- Created a reference Delta table with complex types via SQL
- Inspected its `_delta_log/00000000000000000000.json` to confirm required format
- Uploaded a Delta log with simpleString format -- rejected with `INVALID_JSON_DATA_TYPE`
- Uploaded a Delta log with JSON object format -- read successfully

## Solution

### Spark Path (`build_delta_schema_from_spark`)

Replaced `spark_field.dataType.simpleString()` with `spark_field.dataType.jsonValue()`. Returns:
- Simple types: `"string"`, `"long"`, `"integer"` (strings, same as before)
- Complex types: proper nested dicts matching Delta protocol

### Glue Path (`build_delta_schema_from_glue`)

Added `_parse_hive_type()` -- a pure Python recursive descent parser (no Spark session required) that converts Hive type strings into Delta protocol JSON objects.

For complex types (containing `<`), `_map_glue_to_delta_type()` delegates to `_parse_hive_type()`. Simple types continue using the existing `GLUE_TO_DELTA_TYPE_MAP`.

Grammar handled:
```
type        := array_type | map_type | struct_type | decimal_type | simple_type
array_type  := "array" "<" type ">"
map_type    := "map" "<" type "," type ">"
struct_type := "struct" "<" field ("," field)* ">"
field       := name ":" type
```

Nesting is fully supported (e.g., `array<struct<id:int,tags:array<string>>>`).

### Type Hint Change

The `"type"` field in schema dicts is now `DeltaType = str | dict[str, Any]` since complex types are JSON objects.
