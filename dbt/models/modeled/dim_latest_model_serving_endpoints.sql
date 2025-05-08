with source_entities as (
    select * from {{ ref('stg_system_serving_served_entities') }} -- UPDATED REF
),

ranked_endpoints as (
    select 
        endpoint_id,
        endpoint_name,
        endpoint_config_version,
        created_by,
        workspace_id,
        change_time,
        endpoint_delete_time,
        row_number() over (partition by endpoint_id order by endpoint_config_version desc) as rn
    from source_entities
)

select 
    endpoint_id,
    endpoint_name,
    created_by as endpoint_created_by,
    workspace_id,
    change_time as last_updated_at,
    endpoint_delete_time is not null as is_deleted
from ranked_endpoints
where rn = 1 