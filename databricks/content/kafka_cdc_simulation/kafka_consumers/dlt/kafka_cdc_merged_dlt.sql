-- create a streaming table, then use auto cdc to populate it:
create or refresh streaming table 
  kafka_cdc_merged_raw
  tblproperties('delta.feature.varianttype-preview' = 'supported')
  cluster by auto;

create flow kafka_cdc_merged_raw__cdc_processing
as auto cdc into
  kafka_cdc_merged_raw
from stream(kafka_cdc_raw)
  keys (record_id)
  sequence by record_sequence
  columns record_id, record_sequence, record_payload;

create or replace view
  kafka_cdc_merged

as (
  select
    record_id as id,
    record_payload:`updated_at` as updated_at,
    record_payload:`value` as value,
    record_payload:`payload_string` as payload_string

  from
    kafka_cdc_merged_raw
);