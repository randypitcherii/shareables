{{ config(
    materialized='table',
    persist_docs={'relation': true, 'columns': true},
    databricks_tags={'experiment': 'dbt_v2_compat', 'row': 'r20'},
    tblproperties={'dbt_v2_compat.row': 'r20'},
    tags=['r20_tags_persist_docs']
) }}
select customer_id, customer_name, region from {{ ref('customers') }}
