create or refresh streaming table 
  kafka_cdc_raw
  cluster by auto
  tblproperties('delta.feature.varianttype-preview' = 'supported')

as (
    SELECT
      -- good ol ingestion time
      current_timestamp as ingested_from_kafka_at_timestamp,

      -- parse the cdc records best we can 💪
      try_parse_json(value::string) as kafka_message,
      kafka_message:`record_id`::string as record_id, -- upsert id
      kafka_message:`record_type`::string as record_type, -- insert vs update
      kafka_message:`record_sequence`::string as record_sequence, -- determines the order of records 
      kafka_message:`record_payload` as record_payload, -- actual cdc payload

      -- metadata about the simulation
      kafka_message:`source` as cdc_simulation_source, -- what kind of compute produced it
      kafka_message:`run_started_at` as cdc_simulation_run_started_at_timestamp, -- when the simulation started, which is also good as a run ID

      -- backup information just in case. If we use these, something has gone horribly wrong 💀
      topic as kafka_topic,
      partition as kafka_partition,
      offset as kafka_offset,
      timestamp as arrived_at_kafka_timestamp,
      value::string as kafka_value_string,
      value as kafka_value_raw_bytes


    FROM stream read_kafka(
        `kafka.bootstrap.servers` => secret('randy_pitcher_workspace_kafka_cdc', 'KAFKA_BROKERS'),
        `kafka.security.protocol` => 'SASL_SSL',
        `kafka.sasl.mechanism` => 'SCRAM-SHA-512',
        `kafka.sasl.jaas.config` => CONCAT(
        'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="',
        secret('randy_pitcher_workspace_kafka_cdc', 'KAFKA_USERNAME'),
        '" password="',
        secret('randy_pitcher_workspace_kafka_cdc', 'KAFKA_PASSWORD'),
        '";'
        ),
        `subscribe` => 'rpw_cdc_simulation__sad_lightning',
        `startingTimestamp` => unix_millis(CURRENT_TIMESTAMP - INTERVAL 4 hours)
    )
)