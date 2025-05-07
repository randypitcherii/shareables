with source_audit as (
    select * from {{ ref('stg_system_access_audit') }}
)
select 
    event_time,
    convert_timezone('UTC', 'America/New_York', event_time) as event_time_us_eastern_time,
    user_identity.email as user_email,
    response.status_code as response_status_code,
    response.error_message as error_message,
    if(left(response.status_code, 1) in ('4', '5'), '❌', '✅') as event_status,
    if(left(response.status_code, 1) in ('4', '5'), 1, 0) as err_count,
    service_name,
    action_name,
    request_params,
    source_ip_address,
    workspace_id,
    audit_level,
    identity_metadata,
    user_agent,
    event_id,
    user_identity, -- Carry over the struct for potential further analysis
    response       -- Carry over the struct for potential further analysis
from source_audit 