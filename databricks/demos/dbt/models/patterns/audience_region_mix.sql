{#-
  Run-time dynamic SQL: member counts per timezone, one column per region code
  that ACTUALLY appears in the validated data.

  dbt_utils.get_column_values() queries the warehouse while dbt compiles this
  model, and one_hot(..., agg='sum') turns the result into a pivot. A new region
  in the data becomes a new column on the next build, with no code change. That
  is the right trade for a report, and the wrong one for model features (see
  audience_features.sql).
-#}
{%- set regions = dbt_utils.get_column_values(
    table=ref('audience_members_validated'),
    column='region_code_clean',
    where='region_code_clean is not null',
    order_by='region_code_clean',
    default=[]
) -%}

select
    timezone_clean as timezone,
    count(*) as members
    {%- if regions %},
    {{ one_hot('region_code_clean', regions, prefix='members', agg='sum') }}
    {%- endif %}
from {{ ref('audience_members_validated') }}
where is_valid
group by timezone_clean
