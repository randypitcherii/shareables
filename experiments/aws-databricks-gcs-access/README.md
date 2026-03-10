# AWS Databricks GCS Access Experiment

## Overview

This experiment tests methods for reading data from Google Cloud Storage (GCS) using a Databricks workspace hosted on AWS. The motivation is a customer scenario where publisher data lives in GCS but the customer runs Databricks on AWS.

Three approaches are tested across different Databricks compute types.

## Results Matrix

| Approach | Description | Serverless | Classic Interactive | Job Cluster |
|----------|-------------|------------|---------------------|-------------|
| 1. HMAC / S3-compat (`s3a://`) | Uses GCS HMAC keys with the S3-compatible API via `s3a://` paths and `spark.hadoop.fs.s3a.*` config | FAIL | PASS | untested |
| 2. Python `google-cloud-storage` SDK | Uses the GCP Python SDK to read objects, then loads data into Spark DataFrames | PASS | PASS | untested |
| 3. GCS Connector JAR (`gs://`) | Uses the Hadoop GCS connector with `gs://` paths and `spark.hadoop.google.cloud.*` config | FAIL | PASS | untested |

## Key Findings

### Serverless

- **Serverless blocks all `spark.hadoop.*` config overrides.** Both Approach 1 and Approach 3 fail with `CONFIG_NOT_AVAILABLE` because serverless compute does not allow setting custom Hadoop configuration.
- **The Python SDK is the only working serverless path.** Approach 2 bypasses Spark's Hadoop filesystem layer entirely, using the `google-cloud-storage` library to read objects via HTTP and then constructing Spark DataFrames in Python.

### Classic Interactive

- **All three approaches work on classic compute** when configs are set at the cluster level (not at runtime via `spark.conf.set()`).
- **Cluster-level `spark_conf` is required.** Setting `spark.hadoop.*` configs at runtime via `spark.conf.set()` through Databricks Connect does NOT propagate to the Hadoop filesystem layer. The configs must be baked into the cluster definition.
- **GCS Connector JAR is pre-installed.** Databricks classic clusters include a shaded GCS connector (`shaded.databricks.com.google.cloud.hadoop`), so no custom JAR installation is needed.
- **Use individual SA key fields, not `json.keyfile`.** The `spark.hadoop.google.cloud.auth.service.account.json.keyfile` config expects a file path on disk. Use the individual field configs (`email`, `private.key`, `private.key.id`) with `{{secrets/scope/key}}` references instead.
- **Secrets syntax:** Use `{{secrets/gcs-experiment/key_name}}` in cluster `spark_conf` to inject Databricks secrets at cluster startup time.

## Prerequisites

- A GCP project with Storage and IAM APIs enabled
- [Terraform](https://developer.hashicorp.com/terraform/install) (for provisioning GCP resources)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (configured for your AWS workspace)
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- `gcloud` CLI (authenticated with `gcloud auth login` and `gcloud auth application-default login`)

## Setup

1. **Provision GCP resources with Terraform:**

   ```bash
   cd terraform
   terraform init
   terraform apply
   ```

   This creates the GCS bucket, service account, HMAC keys, and outputs the necessary credentials.

2. **Create Databricks secrets from Terraform outputs:**

   ```bash
   ./setup_secrets.sh
   ```

   This reads Terraform outputs and populates a `gcs-experiment` secret scope with all required secrets.

3. **Deploy the Databricks Asset Bundle** (creates the classic cluster definition):

   ```bash
   databricks bundle deploy
   ```

4. **Install Python dependencies:**

   ```bash
   uv sync
   ```

## Running the Tests

Each script in `scripts/` corresponds to one approach. Run them with `uv run`.

**Serverless (default):**

```bash
uv run python scripts/01_hmac_s3_compat.py
uv run python scripts/02_gcs_python_sdk.py
uv run python scripts/03_gcs_connector_jar.py
```

**Classic cluster** (set `DATABRICKS_CLUSTER_ID`):

```bash
export DATABRICKS_CLUSTER_ID=<cluster-id-from-bundle-deploy>
uv run python scripts/01_hmac_s3_compat.py
uv run python scripts/02_gcs_python_sdk.py
uv run python scripts/03_gcs_connector_jar.py
```

## Project Structure

```
aws-databricks-gcs-access/
├── databricks.yml            # Databricks Asset Bundle config (cluster + spark_conf)
├── pyproject.toml            # Python project and dependencies
├── setup_secrets.sh          # Populates Databricks secrets from Terraform outputs
├── scripts/
│   ├── _common.py            # Shared constants and helpers
│   ├── 01_hmac_s3_compat.py  # Approach 1: HMAC/S3-compatible
│   ├── 02_gcs_python_sdk.py  # Approach 2: Python SDK
│   └── 03_gcs_connector_jar.py  # Approach 3: GCS Connector JAR
└── terraform/
    ├── main.tf               # GCP provider config
    ├── variables.tf          # GCP project, region, bucket name
    ├── outputs.tf            # Credential outputs (marked sensitive)
    ├── gcs.tf                # Bucket + sample data
    ├── hmac.tf               # HMAC key pair
    └── service_account.tf    # Service account + IAM bindings
```
