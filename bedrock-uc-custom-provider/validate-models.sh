#!/usr/bin/env bash
#
# Models-matrix check: invoke each verified Bedrock model through the UC AI
# Gateway custom provider and print pass/fail. Re-run this whenever Bedrock's
# catalog may have drifted.
#
# Claude Fable 5 is included deliberately: on accounts that have NOT run
#   aws bedrock put-account-data-retention --mode provider_data_share
# it fails with a data-retention error. The script recognizes that error and
# reports it as the known account-level caveat rather than a hard failure.
set -uo pipefail

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

# Model IDs verified live on 2026-07-23 in us-east-1 (see README for details).
MODELS=(
  "us.anthropic.claude-sonnet-5|Claude Sonnet 5"
  "us.anthropic.claude-opus-4-8|Claude Opus 4.8"
  "openai.gpt-oss-120b-1:0|GPT-OSS 120B"
  "zai.glm-5|GLM 5"
  "global.amazon.nova-2-lite-v1:0|Nova 2 Lite"
  "us.anthropic.claude-fable-5|Claude Fable 5 (needs data-retention opt-in)"
)

REQUEST_BODY='{"messages":[{"role":"user","content":[{"text":"Reply with the single word: pong"}]}],"inferenceConfig":{"maxTokens":200}}'

PASS=0
FAIL=0
RESP="$(mktemp)"
trap 'rm -f "$RESP"' EXIT

printf '%-38s %-8s %s\n' "MODEL_ID" "STATUS" "DETAIL"
printf '%-38s %-8s %s\n' "--------" "------" "------"

for entry in "${MODELS[@]}"; do
  MODEL_ID="${entry%%|*}"
  LABEL="${entry##*|}"

  : > "$RESP"
  METRICS="$(curl -sS -o "$RESP" -w '%{http_code} %{time_total}' \
    -X POST "${DATABRICKS_HOST}/ai-gateway/model/${MODEL_ID}/converse" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Databricks-Model-Provider-Service: ${FQN}" \
    -H "Content-Type: application/json" \
    -d "$REQUEST_BODY")" || METRICS="000 0"
  HTTP_CODE="${METRICS%% *}"
  ELAPSED="${METRICS##* }"

  # Classify: 0=pass, 2=known Fable 5 data-retention caveat, 1=fail.
  DETAIL="$(python3 - "$RESP" "$HTTP_CODE" <<'PY'
import json, sys
raw = open(sys.argv[1]).read()
code = sys.argv[2]
try:
    doc = json.loads(raw)
except json.JSONDecodeError:
    doc = {}
if code.startswith("2"):
    try:
        # Reasoning models (e.g. gpt-oss) emit a reasoningContent block before
        # the text block — take the first block that carries text.
        blocks = doc["output"]["message"]["content"]
        text = next(b["text"] for b in blocks if "text" in b)
        print(f"replied: {text.strip()[:60]!r}")
        sys.exit(0)
    except (KeyError, StopIteration, TypeError):
        print("HTTP 2xx but unexpected response shape")
        sys.exit(1)
msg = str(doc.get("message", doc.get("Message", raw)))[:200]
if "data retention" in msg.lower():
    print("blocked by account data-retention mode (expected — see README)")
    sys.exit(2)
print(f"HTTP {code}: {msg}")
sys.exit(1)
PY
)"
  CLASS=$?

  case "$CLASS" in
    0) STATUS="PASS"; PASS=$((PASS + 1)) ;;
    2) STATUS="SKIP"; ;;
    *) STATUS="FAIL"; FAIL=$((FAIL + 1)) ;;
  esac
  printf '%-38s %-8s %4.1fs  %s\n' "$MODEL_ID" "$STATUS" "$ELAPSED" "$DETAIL"

  if [[ "$CLASS" == "2" ]]; then
    cat <<'EOF'
    ^ Claude Fable 5 (Mythos-class) requires the AWS account to opt into
      provider data sharing before Bedrock will serve it. One-time fix by an
      AWS account admin:
        aws bedrock put-account-data-retention --mode provider_data_share
      This is account-level, not request-level — no header or
      additionalModelRequestFields works around it.
EOF
  fi
done

echo
echo "passed: ${PASS}  failed: ${FAIL}  (SKIP = known account-level caveat, not counted as failure)"
[[ "$FAIL" -eq 0 ]] || exit 1
