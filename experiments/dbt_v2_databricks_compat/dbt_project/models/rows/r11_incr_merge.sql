{{ config(materialized='incremental', incremental_strategy='merge', unique_key='event_id', tags=['r11_incr_merge']) }}
select * from {{ ref('events') }} where {{ batch_filter() }}
