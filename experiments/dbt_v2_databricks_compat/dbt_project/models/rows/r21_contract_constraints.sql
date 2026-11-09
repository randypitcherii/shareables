{{ config(materialized='table', tags=['r21_contract_constraints']) }}
select cast(customer_id as int) as customer_id, cast(customer_name as string) as customer_name
from {{ ref('customers') }}
