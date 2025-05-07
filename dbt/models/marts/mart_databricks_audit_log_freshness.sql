with source_audit as (
    select event_time from {{ ref('stg_system_access_audit') }}
)
select
    timestampdiff(
        second, 
        max(event_time), 
        current_timestamp()
    ) as seconds_since_last_event,
    seconds_since_last_event / 60.0 as minutes_since_last_event,
    minutes_since_last_event / 60.0 as hours_since_last_event,
    case 
        when hours_since_last_event > 1 
            then round(hours_since_last_event) || ' hours ago'
        when hours_since_last_event = 1 
            then '1 hour ago'
        when minutes_since_last_event > 1 
            then round(minutes_since_last_event) || ' minutes ago'
        when minutes_since_last_event = 1 
            then '1 minute ago'
        when seconds_since_last_event = 1 
            then '1 second ago'
        else 
            round(seconds_since_last_event) || ' seconds ago'
    end as relative_freshness,
    'Latest event: ' || relative_freshness as relative_freshness_message,
    convert_timezone('UTC', 'America/New_York', current_timestamp()) as now_us_eastern_time,
    'Last refreshed: ' || date_format(now_us_eastern_time, 'yyyy-MM-dd h:mm a') || ' ET' as last_refreshed_message
from source_audit 