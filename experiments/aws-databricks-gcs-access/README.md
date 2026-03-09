# AWS Databricks GCS Access Experiment

## Overview

This experiment tests methods for reading data from Google Cloud Storage (GCS) using a Databricks workspace hosted on AWS. The motivation is a customer scenario where publisher data lives in GCS but the customer runs Databricks on AWS.

Three approaches are tested across different Databricks compute types.

## Results Matrix

| Approach | Description | Serverless | Classic Interactive | Job Cluster |
|----------|-------------|------------|---------------------|-------------|
| 1. HMAC / S3-compat (`s3a://`) | Uses GCS HMAC keys with the S3-compatible API via `s3a://` paths and `spark.hadoop.fs.s3a.*` config | FAIL | untested | untested |
| 2. Python `google-cloud-storage` SDK | Uses the GCP Python SDK to read objects, then loads data into Spark DataFrames | PASS | untested | untested |
| 3. GCS Connector JAR (`gs://`) | Uses the Hadoop GCS connector with `gs://` paths and `spark.hadoop.google.cloud.*` config | FAIL | untested | untested |

## Key Findings

- **Serverless blocks all `spark.hadoop.*` config overrides.** Both Approach 1 and Approach 3 fail with `CONFIG_NOT_AVAILABLE` because serverless compute does not allow setting custom Hadoop configuration.
- **The Python SDK is the only working serverless path.** Approach 2 bypasses Spark's Hadoop filesystem layer entirely, using the `google-cloud-storage` library to read objects and then constructing Spark DataFrames in Python.

## Prerequisites

- A GCP project with a GCS bucket and service account
- [Terraform](https://developer.hashicorp.com/terraform/install) (for provisioning GCP resources)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (configured for your AWS workspace)
- [uv](https://docs.astral.sh/uv/) (Python package manager)

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

   This reads Terraform outputs and populates a `gcs-experiment` secret scope with `hmac_access_id`, `hmac_secret`, and `sa_key_json`.

3. **Install Python dependencies:**

   ```bash
   uv sync
   ```

## Running the Tests

Each script in `scripts/` corresponds to one approach. Run them with `uv run`:

```bash
# Approach 1: HMAC / S3-compatible access
uv run python scripts/01_hmac_s3_compat.py

# Approach 2: Python google-cloud-storage SDK
uv run python scripts/02_gcs_python_sdk.py

# Approach 3: GCS Connector JAR
uv run python scripts/03_gcs_connector_jar.py
```

Equivalent notebook versions are available in `notebooks/` for interactive use in the Databricks workspace.

## Project Structure

```
aws-databricks-gcs-access/
├── databricks.yml            # Databricks Asset Bundle config
├── pyproject.toml            # Python project and dependencies
├── uv.lock                   # Locked dependency versions
├── setup_secrets.sh          # Populates Databricks secrets from Terraform outputs
├── notebooks/
│   ├── 01_hmac_s3_compat.py
│   ├── 02_gcs_python_sdk.py
│   └── 03_gcs_connector_jar.py
├── scripts/
│   ├── 01_hmac_s3_compat.py
│   ├── 02_gcs_python_sdk.py
│   └── 03_gcs_connector_jar.py
└── terraform/
    ├── main.tf
    ├── variables.tf
    ├── outputs.tf
    ├── gcs.tf
    ├── hmac.tf
    └── service_account.tf
```
