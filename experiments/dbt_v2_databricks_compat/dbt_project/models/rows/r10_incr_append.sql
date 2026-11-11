{{ config(materialized='incremental', incremental_strategy='append', tags=['r10_incr_append']) }}
select * from {{ ref('events') }} where {{ batch_filter() }}
