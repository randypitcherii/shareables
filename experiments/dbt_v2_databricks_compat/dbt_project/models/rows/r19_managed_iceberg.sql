{{ config(materialized='table', table_format='iceberg', tags=['r19_managed_iceberg']) }}
select * from {{ ref('customers') }}
