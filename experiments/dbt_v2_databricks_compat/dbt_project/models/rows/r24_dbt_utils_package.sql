{{ config(materialized='view', tags=['r24_dbt_utils_package']) }}
select {{ dbt_utils.generate_surrogate_key(['event_id', 'batch_no']) }} as event_key, *
from {{ ref('events') }}
