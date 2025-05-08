select
    event_time,
    user_identity,
    response,
    service_name,
    action_name,
    request_params,
    source_ip_address,
    workspace_id,
    audit_level,
    identity_metadata,
    user_agent,
    event_id
from {{ source('system_access', 'audit') }} 