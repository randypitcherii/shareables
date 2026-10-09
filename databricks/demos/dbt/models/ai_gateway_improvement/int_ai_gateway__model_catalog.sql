{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'destination_model',
    matched_condition = 'DBT_INTERNAL_DEST.price_hash != DBT_INTERNAL_SOURCE.price_hash',
    tblproperties = {'delta.enableRowTracking': 'true', 'delta.enableChangeDataFeed': 'true'}
  )
}}

{#-
  One row per destination_model seen: family, tier, and prices from the
  ai_gateway_model_prices seed (first matching pattern by priority wins),
  plus the reference prices one tier down for overkill savings.

  Resolving the regex match HERE, into an exact-key table, keeps the
  materialized views downstream on equi-joins, which refresh incrementally.
-#}
with models as (
    select distinct coalesce(destination_model, destination_name, endpoint_name) as destination_model
    from {{ ref('stg_ai_gateway__usage') }}
    where service_type is distinct from 'MCP_SERVICE'
),

prices as (
    select * from {{ ref('ai_gateway_model_prices') }}
),

matched as (
    select
        models.destination_model,
        prices.*
    from models
    join prices on models.destination_model rlike prices.model_pattern
    qualify row_number() over (partition by models.destination_model order by prices.priority) = 1
),

-- the price a session would pay one tier down
tier_down as (
    select
        model_tier as ref_tier,
        usd_per_mtok_input  as down_input,
        usd_per_mtok_output as down_output,
        usd_per_mtok_cache_read  as down_cache_read,
        usd_per_mtok_cache_write as down_cache_write,
        model_family as down_family
    from prices
    where is_tier_reference
)

select
    matched.destination_model,
    matched.model_family,
    matched.model_tier,
    matched.model_pattern = '.*' as is_unpriced,
    matched.usd_per_mtok_input,
    matched.usd_per_mtok_output,
    matched.usd_per_mtok_cache_read,
    matched.usd_per_mtok_cache_write,
    tier_down.down_family as tier_down_family,
    tier_down.down_input  as tier_down_usd_per_mtok_input,
    tier_down.down_output as tier_down_usd_per_mtok_output,
    tier_down.down_cache_read  as tier_down_usd_per_mtok_cache_read,
    tier_down.down_cache_write as tier_down_usd_per_mtok_cache_write,
    md5(concat_ws('|',
        matched.model_family, matched.model_tier,
        matched.usd_per_mtok_input, matched.usd_per_mtok_output,
        matched.usd_per_mtok_cache_read, matched.usd_per_mtok_cache_write,
        tier_down.down_family, tier_down.down_input, tier_down.down_output,
        tier_down.down_cache_read, tier_down.down_cache_write
    )) as price_hash
from matched
left join tier_down
    on tier_down.ref_tier = case matched.model_tier
        when 'premium'  then 'standard'
        when 'standard' then 'economy'
    end
