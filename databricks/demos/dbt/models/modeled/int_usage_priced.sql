{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'record_id',
    on_schema_change = 'sync_all_columns'
  )
}}

{#-
  The reference incremental model.

  system.billing.usage is append-only and continuously updated, so a full rebuild
  every run would be wasteful. Instead:
    * full refresh / first run -> bounded to the last `usage_history_days` days
    * incremental runs          -> scan only recent usage, with a lookback window so
                                    late-arriving corrections / restatements get
                                    re-merged on record_id.

  Output grain: one row per usage record, enriched with the effective list price and
  `list_cost` (= usage_quantity * effective list price). This is LIST price, NOT the
  discounted/invoiced spend -- list_prices does not contain account-level discounts.
-#}

{%- set history_days = var('usage_history_days', 90) -%}
{%- set lookback_days = 3 -%}

with usage as (

    select * from {{ ref('stg_billing__usage') }}
    where usage_date >= current_date() - interval {{ history_days }} days

    {% if is_incremental() %}
    and usage_date >= (
        select coalesce(max(usage_date), date '1900-01-01') - interval {{ lookback_days }} days
        from {{ this }}
    )
    {% endif %}

),

list_prices as (
    select * from {{ ref('stg_billing__list_prices') }}
),

priced as (

    select
        usage.*,
        list_prices.currency_code,
        list_prices.effective_list_price,

        -- Cast to double BEFORE multiplying. usage_quantity and the price are both
        -- decimal(38,18); a decimal * decimal here can overflow precision and silently
        -- return NULL. double keeps the arithmetic safe for analytics-grade cost.
        round(
            cast(usage.usage_quantity as double) * cast(list_prices.effective_list_price as double),
            6
        ) as list_cost

    from usage
    left join list_prices
        on  usage.sku_name       = list_prices.sku_name
        and usage.cloud          = list_prices.cloud
        and usage.usage_end_time >= list_prices.price_start_time
        and (list_prices.price_end_time is null or usage.usage_end_time < list_prices.price_end_time)

    -- guarantee exactly one price per usage record even if price windows ever overlap
    qualify row_number() over (
        partition by usage.record_id
        order by list_prices.price_start_time desc
    ) = 1

)

select * from priced
