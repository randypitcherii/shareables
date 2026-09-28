{#-
  The rows to score: short, synthetic text descriptions, deterministic per
  input_id (macros/patterns/synthetic_uniform.sql), so every build scores the
  same inputs and the two inference paths can be compared row for row.

  In a real project this is whatever table needs predictions. Nothing about
  the inference models depends on how this table is built.
-#}
{%- set tones = ['cozy', 'gritty', 'upbeat', 'dark', 'quirky', 'epic', 'heartfelt', 'fast-paced'] -%}
{%- set genres = ['comedy', 'drama', 'documentary', 'thriller', 'cooking show', 'sports recap', 'news brief', 'animated series'] -%}
{%- set topics = ['a small-town bakery', 'college football', 'deep sea creatures', 'a heist gone wrong',
                  'election night', 'backyard gardening', 'a haunted lighthouse', 'street food in Mexico City',
                  'a robot learning to paint', 'the history of jazz'] -%}

with ids as (
    select id + 1 as input_id
    from range({{ var('ai_ml_inference_row_count') }})
)

select
    input_id,
    concat_ws(' ',
        'A',
        {{ synthetic_choice('input_id', 'tone', tones) }},
        {{ synthetic_choice('input_id', 'genre', genres) }},
        'about',
        {{ synthetic_choice('input_id', 'topic', topics) }}
    ) as input_text
from ids
