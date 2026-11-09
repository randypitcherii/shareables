{{ config(
    materialized='incremental',
    incremental_strategy='replace_where',
    incremental_predicates=["event_date >= '2026-03-01'"],
    tags=['r14_incr_replace_where']
) }}
select * from {{ ref('events') }}
where batch_no <= {{ var('batch') }}
{% if is_incremental() %} and event_date >= '2026-03-01' {% endif %}
