{{ config(tags=['patterns']) }}
{#-
  Seed-validation circuit breaker.

  A few invalid timezones or region codes are normal source noise. The
  warn_on_invalid_audience_members test WARNS about them on every build. A LARGE
  share means something upstream broke (a new source format, a bad deploy), so
  this test FAILS the build before bad data spreads. The threshold is a business
  decision (the pattern_max_invalid_share var). The default of 25% fits the
  synthetic data, where about 1 row in 6 is dirty by design.
-#}
{%- set max_invalid_share = var('pattern_max_invalid_share') -%}

select
    count(*) as members,
    count_if(not is_valid) as invalid_members,
    count_if(not is_valid) / count(*) as invalid_share
from {{ ref('audience_members_validated') }}
having count_if(not is_valid) / count(*) > {{ max_invalid_share }}
