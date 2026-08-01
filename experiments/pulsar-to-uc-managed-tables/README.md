# 🌊 Pulsar → Unity Catalog Managed Tables

**The question:** what are the best ways to get events out of Apache Pulsar and into
Unity Catalog **managed** tables (Delta or Iceberg both acceptable)?

This is a live evaluation, not a thought exercise: a real Pulsar broker (with the
Kafka-on-Pulsar protocol handler), one synthetic event stream, and every viable path
run end-to-end into real managed tables. Results land in
[`results/matrix_results.json`](results/matrix_results.json); background research in
[`docs/research/connector-landscape.md`](docs/research/connector-landscape.md).

```
                          ┌──────────────────────────────────────────────┐
                          │  Pulsar standalone (EC2, OSS image + KoP)    │
  00_generate_events ───► │  one topic, two protocols: 6650 (pulsar),    │
                          │  9092 (kafka)                                │
                          └───────┬──────────────┬──────────────┬────────┘
                                  │ kafka        │ pulsar       │ pulsar
                                  ▼              ▼              ▼
                          A: Databricks     B: Databricks   C: PyIceberg via
                          Kafka source      native pulsar   UC Iceberg REST +
                          (GA)              source (preview) credential vending (GA)
                                  │              │              │
                                  ▼              ▼              ▼
                          managed Delta     managed Delta   managed Iceberg
```

## The paths

| Path | Route | Table | Databricks compute in ingest path? | GA status | Verdict |
|---|---|---|---|---|---|
| **A** | KoP (Kafka protocol) → Structured Streaming Kafka source | managed Delta | yes | **GA end-to-end** | _pending run_ |
| **B** | native `format("pulsar")` connector (DBR 14.1+) | managed Delta | yes | source is **Public Preview** | _pending run_ |
| **C** | Pulsar consumer → PyIceberg → UC Iceberg REST catalog | managed Iceberg | no | **GA** (managed Iceberg + credential vending) | _pending run_ |
| **D** | KoP → Kafka Connect Iceberg sink → UC Iceberg REST | managed Iceberg | no | GA surface, needs a Connect worker | documented, not run |
| **E** | pulsar-io-lakehouse sink connector | — | no | n/a | **disqualified** — Iceberg mode has no REST catalog support, so it cannot write UC managed tables |

Framing: with managed tables as the requirement, every path must commit **through Unity
Catalog** — either Databricks compute writing managed Delta, or an external writer speaking
the Iceberg REST catalog with credential vending. Anything that writes storage directly
(path E) can only make external tables and is out.

GA-only rule: the recommended path uses only GA features. Preview surfaces (the native
Pulsar connector, path B) are evaluated as fast-follows, never the critical path.

## Running it

```bash
make install         # uv sync
make auth-check      # Databricks profile + AWS profile
make tf-apply        # EC2 VM: Pulsar standalone + KoP (see terraform/terraform.tfvars.example)
cp template.env dev.env   # then fill from `terraform output`
make pulsar-health

make produce         # synthetic events (EVENT_COUNT / EVENT_RATE_PER_SEC / EVENT_PAYLOAD_BYTES)
make run-path-a      # DAB job: KoP kafka source -> managed Delta
make run-path-b      # DAB job: native pulsar source -> managed Delta
make run-path-c      # local writer: PyIceberg -> managed Iceberg
make verify          # managed-ness + row counts -> results/matrix_results.json

make tf-destroy      # the broker is ephemeral; tear it down
```

Path C prerequisites (one-time, workspace admin):
- metastore **external data access** enabled
- `GRANT EXTERNAL USE SCHEMA ON SCHEMA <catalog>.<schema> TO <principal>`

## Security posture (read before `tf-apply`)

The broker is **unauthenticated plaintext** — this is an ephemeral evaluation rig for
synthetic data, not a reference deployment. You choose the exposure via
`allowed_ingress_cidrs`; destroy the VM when done. A production setup would use TLS +
token/OAuth auth on Pulsar and SASL on the KoP listener.

## Findings

_Per-path numbers are populated from `results/matrix_results.json` after the live run._

### KoP is archived — this is a standing risk for path A (and D)

Path A depends on the StreamNative KoP protocol handler, and KoP is **no longer
maintained**. The repository was archived on 2024-01-24; the final release is
**v3.1.1.1 (2024-01-08)**, which pairs with **Apache Pulsar 3.1.1**. `master` sits at an
unreleased `3.2.0-SNAPSHOT`. There is no KoP build for Pulsar 3.2, 3.3, or 4.x and none
is coming — StreamNative's stated successor is KSN (Kafka on StreamNative), a commercial
layer. So "path A is GA end-to-end" is true of the *Databricks* side only: the Kafka
source is GA, but the Kafka protocol on Pulsar is supplied by a dead open-source project
that pins you to Pulsar 3.1.1. Anyone choosing path A is accepting a frozen broker
version or a commercial dependency. This rig pins `pulsar_version = 3.1.1` /
`kop_version = 3.1.1.1` for exactly that reason.

### KoP needs broker entry metadata, and it must be set before the first write

KoP >= 2.8.0 derives Kafka offsets from Pulsar's broker entry metadata, stamped onto each
BookKeeper entry at write time by
`brokerEntryMetadataInterceptors=...AppendIndexMetadataInterceptor`. If it is unset, the
failure is quiet and misleading: Kafka **Metadata requests still succeed** — clients see
the topic and its partitions — but **ListOffsets fails with `UNKNOWN_SERVER_ERROR`**, so
consumers die resolving `auto.offset.reset` before their first fetch. Spark surfaces this
only as `UnknownServerException ... SQLSTATE: XXKST`, which points nowhere near the real
cause. The metadata cannot be backfilled, so messages produced while it was unset stay
permanently unreadable to Kafka clients: enabling it means re-producing the topic. This
is an easy way to lose a day, and it is a real operational sharp edge for path A.

## Structure

Follows the house experiment shape (see `hive_to_delta` and the UAG experiments):
Makefile command surface, uv-managed Python, terraform for ephemeral infra, DAB-defined
Databricks jobs (profile auth, no PATs), `scripts/` + `results/` + `docs/research/`,
unit vs `infrastructure`-marked tests.
