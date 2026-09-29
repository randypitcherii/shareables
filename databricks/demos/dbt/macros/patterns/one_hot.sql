{#-
  one_hot(column, values, prefix=None, agg=None)

  Dynamic-SQL pattern: generate one column per value instead of hand-writing
  (and hand-maintaining) a wall of CASE expressions.

    one_hot('device_type', ['ctv', 'mobile'])
      -> cast(device_type = 'ctv' as int) as device_type_ctv,
         cast(device_type = 'mobile' as int) as device_type_mobile

  Pass agg='sum' to aggregate instead (a pivot): sum(cast(... as int)) as ...

  Where `values` comes from decides WHEN the SQL is decided:
    * compile time -- a literal list or a project var. The column set is fixed
      and reviewable in the PR diff. Use this for ML features: a model's input
      signature must not change just because new data arrived.
    * run time -- a query, e.g. dbt_utils.get_column_values(ref(...), col).
      The column set follows the data. Good for reporting pivots; wrong for
      model features.

  Values are sanitized into identifiers (lowercase, non-alphanumerics -> _), and
  quoted as string literals in the predicate.
-#}
{% macro one_hot(column, values, prefix=None, agg=None) -%}
  {%- set prefix = prefix if prefix is not none else column -%}
  {%- for value in values -%}
    {%- set identifier = (prefix ~ '_' ~ value) | lower | replace('-', '_') | replace('/', '_') | replace(' ', '_') | replace('.', '_') -%}
    {%- set indicator = "cast(" ~ column ~ " = '" ~ (value | replace("'", "''")) ~ "' as int)" -%}
    {%- if agg %}{{ agg }}({{ indicator }}){% else %}{{ indicator }}{% endif %} as {{ identifier }}
    {%- if not loop.last %},
    {% endif -%}
  {%- endfor -%}
{%- endmacro %}
