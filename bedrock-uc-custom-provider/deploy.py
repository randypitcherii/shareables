#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["databricks-sdk>=0.38"]
# ///
"""Opinionated Bedrock + Unity Catalog AI Gateway setup.

Creates (idempotently):
  1. a catalog (metastore default storage — no storage_root)
  2. a schema
  3. a CUSTOM model provider service pointed at Bedrock, authenticated with a
     Bedrock long-term API key (bearer), with an inference table
  4. a model service for Claude Sonnet 5, with an inference table

then verifies the working invocation path: provider passthrough to Bedrock's
Converse API through the gateway.

The Bedrock key is read from a Databricks secret. Store it once:

    databricks secrets create-scope bedrock
    databricks secrets put-secret bedrock aws_bearer_token_bedrock --string-value 'ABSK...'

Run:  uv run deploy.py

Uses the beta, undocumented `model-provider-services` / `model-services` UC
APIs — not DABs resources yet, hence a plain script with declarative
parameters at the top.
"""

import base64
import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound

# ── Parameters ──────────────────────────────────────────────────────────────
TARGET_CATALOG = "ai_gateway_demo"  # created if missing — metastore default storage
TARGET_SCHEMA = "bedrock"           # created if missing
SECRET_SCOPE = "bedrock"            # Databricks secret holding the ABSK... key
SECRET_KEY = "aws_bearer_token_bedrock"
BEDROCK_REGION = os.environ.get("BEDROCK_REGION", "us-east-1")

PROVIDER_ID = "aws_bedrock"
SERVICE_ID = "sonnet5_bedrock"
MODEL_ID = "us.anthropic.claude-sonnet-5"
NATIVE_API_TYPE = "anthropic/v1/messages"
# ────────────────────────────────────────────────────────────────────────────

SCHEMA_FQN = f"{TARGET_CATALOG}.{TARGET_SCHEMA}"
SCHEMA_PARENT = f"schemas/{SCHEMA_FQN}"
PROVIDER_FQN = f"{SCHEMA_FQN}.{PROVIDER_ID}"
SERVICE_FQN = f"{SCHEMA_FQN}.{SERVICE_ID}"

MPS_API = "/api/2.1/unity-catalog/model-provider-services"
MS_API = "/api/2.1/unity-catalog/model-services"

w = WorkspaceClient()


def ensure_catalog_and_schema():
    try:
        w.catalogs.get(TARGET_CATALOG)
        print(f"catalog {TARGET_CATALOG}: exists")
    except NotFound:
        w.catalogs.create(name=TARGET_CATALOG)  # no storage_root → metastore default storage
        print(f"catalog {TARGET_CATALOG}: created")

    try:
        w.schemas.get(SCHEMA_FQN)
        print(f"schema {SCHEMA_FQN}: exists")
    except NotFound:
        w.schemas.create(name=TARGET_SCHEMA, catalog_name=TARGET_CATALOG)
        print(f"schema {SCHEMA_FQN}: created")


def ensure_gateway_object(api_root, id_param, object_id, fqn, config, label):
    """Create a model-provider-service or model-service if missing.

    Existing objects are left alone: `config.custom` and `config.routing` are
    not updatable (delete + recreate is the only path), and PATCHing the
    inference table onto an object that once had one collides with the
    orphaned `<name>_payload` table. If an object exists with the wrong
    config, delete it (and drop its `_payload` table) and re-run.
    """
    try:
        w.api_client.do("GET", f"{api_root}/{fqn}")
        print(f"{label} {fqn}: exists — leaving as-is")
        return False
    except NotFound:
        pass

    w.api_client.do(
        "POST",
        api_root,
        query={"parent": SCHEMA_PARENT, id_param: object_id},
        body={"config": config},
    )
    print(f"{label} {fqn}: created (inference table {fqn.split('.')[-1]}_payload in {SCHEMA_FQN})")
    return True


def verify_passthrough():
    """Invoke Sonnet 5 through the gateway via provider passthrough.

    This is the invocation that works with a bearer key, and it logs to the
    provider service's inference table and the gateway usage system tables.
    New/changed services take ~1–3 min to reach the gateway's routing cache
    ('Nodes do not exist' in the interim), so retry for up to 5 minutes.
    """
    path = f"/ai-gateway/model/{MODEL_ID}/converse"
    body = {
        "messages": [{"role": "user", "content": [{"text": "Reply with exactly: gateway OK"}]}],
        "inferenceConfig": {"maxTokens": 50},
    }
    headers = {"Databricks-Model-Provider-Service": PROVIDER_FQN}

    deadline = time.time() + 300
    while True:
        try:
            resp = w.api_client.do("POST", path, body=body, headers=headers)
            break
        except DatabricksError as e:
            if "Nodes do not exist" in str(e) and time.time() < deadline:
                print("gateway routing cache not ready yet — retrying in 30s...")
                time.sleep(30)
                continue
            raise

    text = resp["output"]["message"]["content"][0]["text"]
    print(f"\nverification: Sonnet 5 replied through the gateway: {text!r}")
    print(f"usage: {resp.get('usage')}")


def main():
    # secrets/get returns the value base64-encoded
    bedrock_api_key = base64.b64decode(
        w.secrets.get_secret(SECRET_SCOPE, SECRET_KEY).value
    ).decode()

    ensure_catalog_and_schema()

    created = ensure_gateway_object(
        MPS_API,
        "model_provider_service_id",
        PROVIDER_ID,
        PROVIDER_FQN,
        {
            "provider_type": "EXTERNAL_MODEL_PROVIDER_TYPE_CUSTOM",
            "allow_all_targets": True,
            "forward_unmanaged_paths": True,
            "custom": {
                "direct": {
                    "api_key": {"plaintext": bedrock_api_key},
                    "base_url": f"https://bedrock-runtime.{BEDROCK_REGION}.amazonaws.com",
                }
            },
            "targets": [{"model": MODEL_ID, "native_api_types": [NATIVE_API_TYPE]}],
            "inference_table": {"parent": SCHEMA_PARENT},
        },
        "model-provider-service",
    )

    ensure_gateway_object(
        MS_API,
        "model_service_id",
        SERVICE_ID,
        SERVICE_FQN,
        {
            "routing": {
                "destinations": [
                    {
                        "name": "primary",
                        "destination_type": "DESTINATION_TYPE_EXTERNAL_FOUNDATION_MODEL",
                        "traffic_percentage": 100,
                        "external_model_config": {
                            "model_provider_service": f"model-provider-services/{PROVIDER_FQN}",
                            "target": {"model": MODEL_ID, "native_api_types": [NATIVE_API_TYPE]},
                        },
                    }
                ]
            },
            "inference_table": {"parent": SCHEMA_PARENT},
        },
        "model-service",
    )

    if created:
        print("\nnew provider service — the gateway routing cache may need ~1–3 min")
    verify_passthrough()


if __name__ == "__main__":
    sys.exit(main())
