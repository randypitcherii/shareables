select
    endpoint_id,
    endpoint_name,
    endpoint_config_version,
    created_by,
    workspace_id,
    change_time,
    endpoint_delete_time
from {{ source('system_serving', 'served_entities') }} 