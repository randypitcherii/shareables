{{ config(materialized='ephemeral', tags=['r07_ephemeral']) }}
select customer_id, count(*) as event_count from {{ ref('events') }} group by customer_id
