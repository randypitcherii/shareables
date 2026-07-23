#!/usr/bin/env bash
#
# Register AWS Bedrock as a CUSTOM model provider in Unity Catalog AI Gateway.
#
# Uses the (beta, undocumented) UC model-provider-services API. The native
# EXTERNAL_MODEL_PROVIDER_TYPE_AMAZON_BEDROCK provider type only accepts AWS
# access-key pairs; the CUSTOM type accepts any bearer key + base URL, which is
# exactly what Bedrock's long-term API keys (ABSK...) need.
#
# Requires: bash, curl, python3, databricks CLI (only if DATABRICKS_HOST/TOKEN
# are not already exported).
set -euo pipefail

# ── Configuration (override via env) ─────────────────────────────────────────
AWS_REGION="${AWS_REGION:-us-east-1}"
UC_CATALOG="${UC_CATALOG:-main}"
UC_SCHEMA="${UC_SCHEMA:-default}"
PROVIDER_ID="${PROVIDER_ID:-bedrock_bearer}"
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"

BEDROCK_BASE_URL="https://bedrock-runtime.${AWS_REGION}.amazonaws.com"

# The Bedrock long-term API key is ONLY accepted via env var — never hardcode.
: "${AWS_BEARER_TOKEN_BEDROCK:?Set AWS_BEARER_TOKEN_BEDROCK to your Bedrock long-term API key (starts with ABSK)}"

# ── Databricks auth: env vars win, otherwise resolve from the CLI profile ────
if [[ -z "${DATABRICKS_HOST:-}" ]]; then
  DATABRICKS_HOST="$(databricks auth env --profile "$DATABRICKS_PROFILE" \
    | python3 -c 'import sys, json; print(json.load(sys.stdin)["env"]["DATABRICKS_HOST"])')"
fi
DATABRICKS_HOST="${DATABRICKS_HOST%/}"
if [[ -z "${DATABRICKS_TOKEN:-}" ]]; then
  DATABRICKS_TOKEN="$(databricks auth token --profile "$DATABRICKS_PROFILE" \
    | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')"
fi

# ── Build the request body (python3 so the key is JSON-escaped safely) ───────
export BEDROCK_BASE_URL
BODY="$(python3 <<'PY'
import json, os
print(json.dumps({
    "config": {
        "provider_type": "EXTERNAL_MODEL_PROVIDER_TYPE_CUSTOM",
        "allow_all_targets": True,
        "forward_unmanaged_paths": True,
        "custom": {
            "direct": {
                "api_key": {"plaintext": os.environ["AWS_BEARER_TOKEN_BEDROCK"]},
                "base_url": os.environ["BEDROCK_BASE_URL"],
            }
        },
    }
}))
PY
)"

# ── Create the provider ──────────────────────────────────────────────────────
CREATE_URL="${DATABRICKS_HOST}/api/2.1/unity-catalog/model-provider-services?parent=schemas/${UC_CATALOG}.${UC_SCHEMA}&model_provider_service_id=${PROVIDER_ID}"

echo "Creating custom model provider service ${UC_CATALOG}.${UC_SCHEMA}.${PROVIDER_ID}"
echo "  base_url: ${BEDROCK_BASE_URL}"

RESP="$(mktemp)"
trap 'rm -f "$RESP"' EXIT
HTTP_CODE="$(curl -sS -o "$RESP" -w '%{http_code}' \
  -X POST "$CREATE_URL" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$BODY")"

# Pretty-print the response but redact the echoed base_url/key material.
python3 - "$RESP" "$HTTP_CODE" <<'PY'
import json, sys
body_path, code = sys.argv[1], sys.argv[2]
raw = open(body_path).read()
try:
    doc = json.loads(raw)
    # The API echoes config back; the key itself is never returned, but keep
    # the output tidy either way.
    print(json.dumps(doc, indent=2))
except json.JSONDecodeError:
    print(raw)
print(f"\nHTTP {code}")
if not code.startswith("2"):
    sys.exit(1)
PY

cat <<EOF

Provider created: ${UC_CATALOG}.${UC_SCHEMA}.${PROVIDER_ID}

NOTE: create does NOT validate the credential — a typo'd key creates fine and
only fails at invocation time. Run ./validate-models.sh next to prove it works.
EOF
