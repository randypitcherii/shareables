{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='event_id', tags=['r12_incr_delete_insert']) }}
select * from {{ ref('events') }} where {{ batch_filter() }}
