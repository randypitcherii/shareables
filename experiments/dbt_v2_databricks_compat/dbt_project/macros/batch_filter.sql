{#- Rows visible at the current `batch`, and on an incremental pass only the
    rows newer than what the target already holds. Shared by every
    incremental-strategy row so they differ only in strategy. -#}
{% macro batch_filter() -%}
batch_no <= {{ var('batch') }}
{%- if is_incremental() %}
  and batch_no > (select coalesce(max(batch_no), 0) from {{ this }})
{%- endif %}
{%- endmacro %}
