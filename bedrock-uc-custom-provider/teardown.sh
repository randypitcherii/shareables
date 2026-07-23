#!/usr/bin/env bash
#
# Delete the Bedrock custom model provider service created by setup.sh.
set -euo pipefail

# ── Configuration (override via env — must match setup.sh) ───────────────────
UC_CATALOG="${UC_CATALOG:-main}"
UC_SCHEMA="${UC_SCHEMA:-default}"
PROVIDER_ID="${PROVIDER_ID:-bedrock_bearer}"
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"

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

FQN="${UC_CATALOG}.${UC_SCHEMA}.${PROVIDER_ID}"
DELETE_URL="${DATABRICKS_HOST}/api/2.1/unity-catalog/model-provider-services/${FQN}"

echo "Deleting custom model provider service ${FQN}"

RESP="$(mktemp)"
trap 'rm -f "$RESP"' EXIT
HTTP_CODE="$(curl -sS -o "$RESP" -w '%{http_code}' \
  -X DELETE "$DELETE_URL" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}")"

if [[ "$HTTP_CODE" == 2* ]]; then
  echo "Deleted (HTTP ${HTTP_CODE})."
else
  echo "Delete failed (HTTP ${HTTP_CODE}):"
  python3 - "$RESP" <<'PY'
import json, sys
raw = open(sys.argv[1]).read()
try:
    print(json.dumps(json.loads(raw), indent=2))
except json.JSONDecodeError:
    print(raw)
PY
  exit 1
fi
