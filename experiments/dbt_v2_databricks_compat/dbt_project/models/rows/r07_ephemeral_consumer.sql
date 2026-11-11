{{ config(materialized='view', tags=['r07_ephemeral']) }}
select * from {{ ref('r07_ephemeral_base') }}
