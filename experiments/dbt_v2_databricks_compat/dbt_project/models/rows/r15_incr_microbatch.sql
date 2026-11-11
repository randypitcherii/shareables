{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_ts',
    batch_size='month',
    begin='2026-01-01',
    tags=['r15_incr_microbatch']
) }}
select * from {{ ref('events') }} where batch_no <= {{ var('batch') }}
