{#- The pattern under test: ingest files that land in a Unity Catalog volume
    with a streaming table, owned by dbt. Re-running must pick up only the new
    files. -#}
{{ config(materialized='streaming_table', tags=['r16_streaming_table']) }}
select *, _metadata.file_name as source_file
from stream read_files(
  '{{ var("landing_path") }}/orders',
  format => 'csv',
  header => true,
  schema => 'order_id int, customer_id int, order_amount decimal(10,2)'
)
