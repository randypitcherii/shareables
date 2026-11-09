{{ config(materialized='view', tags=['r05_view']) }}
select * from {{ ref('events') }} where batch_no <= {{ var('batch') }}
