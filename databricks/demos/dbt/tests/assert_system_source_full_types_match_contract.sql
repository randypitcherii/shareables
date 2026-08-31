{#-
  Structure-drift tripwire for the system catalog (WARN tier).

  Companion to assert_system_source_columns_match_contract.sql. This one warns
  when a contracted column still exists with the same base type but its FULL
  type string drifted -- almost always Databricks adding fields inside a struct
  or map. That is additive and rarely breaks the metric views, but it is exactly
  the "unexpected structure change" we want surfaced, so: warn, review the new
  shape, then refresh the row in seeds/system_table_schema_contract.csv.
-#}
{{ config(severity='warn') }}

with contract as (
    select
        table_schema,
        table_name,
        column_name,
        data_type                               as expected_full_type,
        regexp_extract(data_type, '^[a-z]+', 0) as expected_base_type
    from {{ ref('system_table_schema_contract') }}
),

actual as (
    select
        table_schema,
        table_name,
        column_name,
        -- see assert_system_source_columns_match_contract.sql: base type must come
        -- from full_data_type (data_type reports Spark-internal names like LONG)
        regexp_extract(lower(full_data_type), '^[a-z]+', 0) as actual_base_type,
        lower(full_data_type)                                as actual_full_type
    from {{ source('system_information_schema', 'columns') }}
)

select
    contract.table_schema,
    contract.table_name,
    contract.column_name,
    contract.expected_full_type,
    actual.actual_full_type
from contract
inner join actual
    on  actual.table_schema = contract.table_schema
    and actual.table_name   = contract.table_name
    and actual.column_name  = contract.column_name
where actual.actual_base_type = contract.expected_base_type
  and actual.actual_full_type != contract.expected_full_type
