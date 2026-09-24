{#-
  Synthetic stand-in for a raw source table: one row per audience member.

  Every pattern example builds from this table, so the whole folder runs on a
  fresh clone without access to any real data. Values are deterministic per
  member_id (see macros/patterns/synthetic_uniform.sql), so reruns are identical.

  It is dirty ON PURPOSE, like real source data:
    * region codes arrive as aliases ('UK', 'usa ') and as junk ('ZZ', '??')
    * timezones arrive mostly valid, with a typo ('US/Pacifc') and a non-IANA
      abbreviation ('EST5')
  audience_members_validated.sql cleans and validates them against seeds.

  `in_segment` is the label for the logistic regression example. It comes from
  a known logistic function of the features plus noise, so the trained model
  has a real signal to recover.
-#}
{%- set regions = [
    'US', 'US', 'US', 'US', 'US', 'US', 'us ', 'USA',
    'CA', 'CA', 'GB', 'GB', 'UK', 'DE', 'DE', 'MX',
    'BR', 'JP', 'AU', 'FR', 'IN', 'ZZ', '??', 'Germany'
] -%}
{%- set timezones = [
    'America/New_York', 'America/New_York', 'America/New_York', 'America/Chicago',
    'America/Chicago', 'America/Denver', 'America/Los_Angeles', 'America/Los_Angeles',
    'America/Toronto', 'America/Mexico_City', 'America/Sao_Paulo', 'Europe/London',
    'Europe/Berlin', 'Europe/Paris', 'Asia/Tokyo', 'Asia/Kolkata',
    'Australia/Sydney', 'UTC', 'US/Pacifc', 'EST5'
] -%}
{%- set interests = [
    'sports', 'football', 'basketball', 'news', 'politics', 'cooking', 'travel',
    'comedy', 'drama', 'documentary', 'kids', 'animation', 'music', 'gaming',
    'finance', 'fitness', 'home', 'garden', 'cars', 'science'
] -%}

with members as (
    select id + 1 as member_id
    from range({{ var('pattern_member_count') }})
),

features as (
    select
        member_id,
        {{ synthetic_choice('member_id', 'region', regions) }} as region_code_raw,
        {{ synthetic_choice('member_id', 'tz', timezones) }} as timezone_raw,
        {{ synthetic_choice('member_id', 'device', var('pattern_device_types')) }} as device_type,
        cast(floor({{ synthetic_uniform('member_id', 'sessions') }} * 61) as int) as sessions_30d,
        round({{ synthetic_uniform('member_id', 'watch') }} * 180, 1) as avg_watch_minutes,
        cast(floor({{ synthetic_uniform('member_id', 'tenure') }} * 1500) as int) + 1 as days_since_signup,
        concat_ws(' ',
            {{ synthetic_choice('member_id', 'interest_1', interests) }},
            {{ synthetic_choice('member_id', 'interest_2', interests) }},
            {{ synthetic_choice('member_id', 'interest_3', interests) }}
        ) as interests_text,
        {{ synthetic_uniform('member_id', 'label_noise') }} as label_draw
    from members
)

select
    member_id,
    region_code_raw,
    timezone_raw,
    device_type,
    sessions_30d,
    avg_watch_minutes,
    days_since_signup,
    interests_text,
    -- the "true" model the logistic regression should recover:
    --   logit = -3 + 0.06*sessions + 0.012*watch + 0.9*[ctv] - 0.0008*tenure
    label_draw < 1 / (1 + exp(-(
        -3.0
        + 0.06 * sessions_30d
        + 0.012 * avg_watch_minutes
        + 0.9 * cast(device_type = 'ctv' as int)
        - 0.0008 * days_since_signup
    ))) as in_segment
from features
