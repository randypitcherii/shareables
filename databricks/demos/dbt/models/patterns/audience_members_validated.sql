{#-
  Seed-based validation pattern.

  Reference data (IANA timezones, ISO 3166-1 region codes) lives in version-
  controlled seeds under seeds/patterns/, so the list of valid values is
  reviewed like code and is the same in every environment.

  Every row is kept -- nothing is silently dropped. Each gets:
    * the normalized value (Python UDF: functions/patterns/normalize_region_code.py)
    * a validity flag from a left join to the seed
    * a `_clean` column that is NULL when the value is invalid
  Downstream models filter on `is_valid`. Tests in _patterns__models.yml warn on
  every invalid row and fail the build if the invalid share passes a threshold.
-#}
with members as (
    select * from {{ ref('synth_audience_members') }}
),

normalized as (
    select
        *,
        {{ function('normalize_region_code') }}(region_code_raw) as region_code,
        trim(timezone_raw) as timezone
    from members
),

validated as (
    select
        normalized.*,
        regions.region_code is not null as is_valid_region_code,
        timezones.timezone is not null as is_valid_timezone
    from normalized
    left join {{ ref('ref_iso_region_codes') }} as regions
        on normalized.region_code = regions.region_code
    left join {{ ref('ref_iana_timezones') }} as timezones
        on normalized.timezone = timezones.timezone
)

select
    member_id,
    region_code_raw,
    region_code,
    case when is_valid_region_code then region_code end as region_code_clean,
    is_valid_region_code,
    timezone_raw,
    case when is_valid_timezone then timezone end as timezone_clean,
    is_valid_timezone,
    is_valid_region_code and is_valid_timezone as is_valid,
    device_type,
    sessions_30d,
    avg_watch_minutes,
    days_since_signup,
    interests_text,
    in_segment
from validated
