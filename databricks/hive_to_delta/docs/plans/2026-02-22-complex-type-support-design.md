# Complex Data Type Support for Hive-to-Delta Conversion

## Problem

Two bugs prevent hive_to_delta from correctly handling complex data types (arrays, maps, structs):

1. **Glue path silent corruption**: Parameterized complex types from AWS Glue (e.g., `array<string>`, `map<string,int>`) are silently mapped to `"string"` in the Delta schema. Root cause: `_map_glue_to_delta_type()` doesn't handle `<` angle brackets.

2. **Non-compliant schema format**: Both Glue and Spark paths produce flat string representations (`"array<string>"`) instead of Delta protocol JSON objects. Databricks rejects this at read time with `INVALID_JSON_DATA_TYPE`.

## Cluster Validation

Tested on `fe-vm-fe-randy-pitcher-workspace.cloud.databricks.com`:

- Created a reference Delta table with complex types via SQL
- Inspected its `_delta_log/00000000000000000000.json`
- Confirmed Delta protocol requires nested JSON objects:
  - Array: `{"type": "array", "elementType": "string", "containsNull": true}`
  - Map: `{"type": "map", "keyType": "string", "valueType": "integer", "valueContainsNull": true}`
  - Struct: `{"type": "struct", "fields": [...]}`
- Uploaded a Delta log with simpleString format — **rejected** with `INVALID_JSON_DATA_TYPE`
- Uploaded a Delta log with JSON object format — **read successfully**

## Solution

Use PySpark's built-in type system, which already handles parsing and serialization:

### Spark Path (`build_delta_schema_from_spark`)

Replace `spark_field.dataType.simpleString()` with `spark_field.dataType.jsonValue()`. This returns:
- Simple types: `"string"`, `"long"`, `"integer"` (strings — same as before)
- Complex types: proper nested dicts matching Delta protocol

### Glue Path (`build_delta_schema_from_glue`)

For complex types (containing `<`), parse the Hive type string using PySpark's `_parse_datatype_string()`, then call `.jsonValue()`:
- `_parse_datatype_string("array<string>").jsonValue()` returns `{"type": "array", "elementType": "string", "containsNull": true}`
- Simple types continue using the existing `GLUE_TO_DELTA_TYPE_MAP` dictionary

### Type Hint Change

The `"type"` field in schema dicts changes from `str` to `str | dict` since complex types are now JSON objects.

## Tasks

1. Update `_map_glue_to_delta_type()` to parse complex types via PySpark
2. Update `build_delta_schema_from_spark()` to use `jsonValue()`
3. Update unit tests for new format (mock `jsonValue()` instead of `simpleString()`)
4. Add comprehensive complex type tests (nested arrays, maps, structs)
5. Run full test suite to verify no regressions
6. Verify on Databricks cluster with an end-to-end test
