{#-
  Showback example: attribute daily list cost to the `Owner` custom tag.

  custom_tags is a user-controlled map, so this doubles as a tagging-hygiene report --
  everything landing in '(untagged)' is cost you can't yet attribute to an owner.
-#}
with tagged as (
    select
        coalesce(custom_tags['Owner'], '(untagged)') as owner,
        usage_date,
        cloud,
        currency_code,
        usage_quantity,
        list_cost
    from {{ ref('int_usage_priced') }}
)

select
    owner,
    usage_date,
    cloud,
    currency_code,
    sum(usage_quantity) as usage_quantity,
    sum(list_cost)      as list_cost
from tagged
group by owner, usage_date, cloud, currency_code
