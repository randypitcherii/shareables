{#-
  Structure-drift tripwire for the system catalog (ERROR tier).

  seeds/system_table_schema_contract.csv pins every system.* column the metric
  views were validated against. This test fails when any of those columns has
  DISAPPEARED from system.information_schema, or its base type (string, bigint,
  struct, ...) has changed -- either one means Databricks restructured a system
  table under us and the metric views need review, not just a re-run.

  A table whose columns ALL go missing usually means the table itself was
  dropped/renamed -- or this principal lost SELECT on it (information_schema
  only shows objects you can access).

  Struct-internal evolution is deliberately NOT an error -- that is the WARN
  tier in assert_system_source_full_types_match_contract.sql.
-#}
with contract as (
    select
        table_schema,
        table_name,
        column_name,
        data_type                                   as expected_full_type,
        regexp_extract(data_type, '^[a-z]+', 0)     as expected_base_type
    from {{ ref('system_table_schema_contract') }}
),

actual as (
    select
        table_schema,
        table_name,
        column_name,
        -- base type comes from full_data_type: information_schema.data_type reports
        -- Spark-internal names (LONG, BYTE) while full_data_type uses the SQL names
        -- (bigint, tinyint) the contract is written in
        regexp_extract(lower(full_data_type), '^[a-z]+', 0) as actual_base_type
    from {{ source('system_information_schema', 'columns') }}
)

select
    contract.table_schema,
    contract.table_name,
    contract.column_name,
    contract.expected_base_type,
    actual.actual_base_type,
    case
        when actual.column_name is null then 'column missing from system catalog'
        else 'column base type changed'
    end as violation
from contract
left join actual
    on  actual.table_schema = contract.table_schema
    and actual.table_name   = contract.table_name
    and actual.column_name  = contract.column_name
where actual.column_name is null
   or actual.actual_base_type != contract.expected_base_type
