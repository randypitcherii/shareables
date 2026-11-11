{{ config(materialized='incremental', incremental_strategy='insert_overwrite', partition_by=['event_date'], tags=['r13_incr_insert_overwrite']) }}
select * from {{ ref('events') }} where {{ batch_filter() }}
