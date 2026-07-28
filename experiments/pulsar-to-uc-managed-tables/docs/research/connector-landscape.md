# Research: Pulsar → Unity Catalog managed tables — connector landscape

Point-in-time findings (July 2026) that shaped the evaluation matrix. Verify GA/preview
status against current docs before relying on this.

## Candidate inventory

### Databricks native Pulsar connector (Structured Streaming source)
- `spark.readStream.format("pulsar")`, available in DBR 14.1+.
- **Public Preview** as of June 2026 ([docs](https://docs.databricks.com/aws/en/connect/streaming/pulsar)).
- Also usable as a source in Lakeflow Declarative Pipelines ([concepts](https://docs.databricks.com/aws/en/ldp/concepts)).
- Consequence: cannot be the GA-critical path today; evaluated as path B / fast-follow.

### Kafka protocol on Pulsar (KoP) + Databricks Kafka source
- KoP is an Apache-2.0 protocol handler NAR ([repo](https://github.com/streamnative/kop)) that
  runs on the OSS `apachepulsar/pulsar` image — no commercial subscription required.
- Latest OSS release at time of writing: v3.1.1.1, pairing with Pulsar 3.1.1 (KoP versions
  must match the broker's Pulsar version).
- `entryFormat=pulsar` lets Pulsar-protocol producers and Kafka-protocol consumers share one
  topic (at a per-message conversion cost).
- Databricks' Kafka source is long-GA → KoP + Kafka source is the **fully-GA Databricks
  compute path** today. Evaluated as path A.

### Unity Catalog managed Iceberg + Iceberg REST catalog (external writers)
- Managed Iceberg went **GA May 2026** ([release notes](https://docs.databricks.com/aws/en/release-notes/product/2026/may)).
- External engines can **read, write, and create** managed Iceberg tables through
  `/api/2.1/unity-catalog/iceberg-rest` with credential vending
  ([docs](https://docs.databricks.com/aws/en/external-access/iceberg)).
- Requirements: metastore `external data access` enabled; writer principal granted
  `EXTERNAL USE SCHEMA`; current-ish Iceberg client (credential refresh bugs in older ones).
- Writer options: PyIceberg (evaluated as path C), Flink Iceberg sink, Kafka Connect Iceberg
  sink (path D, documented). Foreign Iceberg tables are read-only — only managed Iceberg is
  externally writable.

### pulsar-io-lakehouse sink connector — disqualified
- Actively maintained by StreamNative (v4.0.3.x releases in 2026), supports Delta / Iceberg /
  Hudi sinks ([docs](https://docs.streamnative.io/hub/connector-lakehouse-sink-v4.0)).
- **Iceberg mode only supports `hadoopCatalog` and `hiveCatalog`** — no REST catalog support,
  so it cannot commit through UC and cannot produce *managed* tables. Its Delta mode writes
  files directly to storage → external table at best.
- Disqualified for this evaluation's managed-tables requirement; would re-qualify if REST
  catalog support lands.

## Broker hosting notes
- OSS image `apachepulsar/pulsar:<ver>` standalone is sufficient for evaluation; KoP NAR is
  mounted into `/pulsar/protocols` and enabled via `messagingProtocols=kafka`.
- `advertisedAddress` and `kafkaAdvertisedListeners` must be the VM's public IP or external
  clients fail after topic lookup/metadata redirect.
- StreamNative's platform Docker images are license-gated — avoided; everything here is
  Apache-licensed OSS.
