#!/usr/bin/env bash
# Setup script to create Databricks secret scope and populate secrets
# from Terraform outputs. Run this AFTER terraform apply.
#
# Usage: ./setup_secrets.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$SCRIPT_DIR/terraform"

echo "Reading Terraform outputs..."
HMAC_ACCESS_ID=$(terraform -chdir="$TF_DIR" output -raw hmac_access_id)
HMAC_SECRET=$(terraform -chdir="$TF_DIR" output -raw hmac_secret)
SA_KEY_BASE64=$(terraform -chdir="$TF_DIR" output -raw sa_key_json_base64)

# Decode the base64-encoded SA key
SA_KEY_JSON=$(echo "$SA_KEY_BASE64" | base64 --decode)

SCOPE="gcs-experiment"

echo "Creating secret scope '$SCOPE'..."
databricks secrets create-scope "$SCOPE" 2>/dev/null || echo "Scope '$SCOPE' already exists"

echo "Setting secrets..."
echo -n "$HMAC_ACCESS_ID" | databricks secrets put-secret "$SCOPE" hmac_access_id
echo -n "$HMAC_SECRET" | databricks secrets put-secret "$SCOPE" hmac_secret
echo -n "$SA_KEY_JSON" | databricks secrets put-secret "$SCOPE" sa_key_json

echo ""
echo "Done. Secrets configured:"
databricks secrets list-secrets "$SCOPE"
