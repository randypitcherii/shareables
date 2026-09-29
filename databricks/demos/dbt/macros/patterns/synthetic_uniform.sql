{#-
  synthetic_uniform(id_column, salt) -> a DOUBLE in [0, 1) that is a pure
  function of (id_column, salt).

  The pattern examples need synthetic data that is IDENTICAL on every build, in
  every environment, so a test failure means the code changed -- not the dice.
  rand(seed) is not safe for that: its sequence depends on how Spark partitions
  the input. A hash of the row key and a per-column salt is.
-#}
{% macro synthetic_uniform(id_column, salt) -%}
  (pmod(xxhash64({{ id_column }}, '{{ salt }}'), 1000000) / 1000000.0)
{%- endmacro %}


{#-
  synthetic_choice(id_column, salt, values) -> one element of `values`, picked
  uniformly and deterministically per row. Repeat a value in the list to weight it.
-#}
{% macro synthetic_choice(id_column, salt, values) -%}
  element_at(
    array({% for v in values %}'{{ v }}'{{ ", " if not loop.last }}{% endfor %}),
    cast(floor({{ synthetic_uniform(id_column, salt) }} * {{ values | length }}) as int) + 1
  )
{%- endmacro %}
