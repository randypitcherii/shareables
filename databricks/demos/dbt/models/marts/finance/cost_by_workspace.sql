{#-
  Daily list cost by workspace.

  workspace_id is NULL for account-level usage (some storage / network / serverless
  features are billed at the account, not a workspace). We surface that as an explicit
  '(account-level)' bucket so those costs are attributed rather than silently dropped.
-#}
with usage as (
    select
        coalesce(workspace_id, '(account-level)') as workspace_id,
        usage_date,
        cloud,
        currency_code,
        usage_quantity,
        list_cost
    from {{ ref('int_usage_priced') }}
)

select
    workspace_id,
    usage_date,
    cloud,
    currency_code,
    sum(usage_quantity) as usage_quantity,
    sum(list_cost)      as list_cost
from usage
group by workspace_id, usage_date, cloud, currency_code
