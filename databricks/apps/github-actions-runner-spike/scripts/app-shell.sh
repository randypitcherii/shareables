#!/usr/bin/env bash
# Run a bash command inside the deployed app container via /api/v1/shell/run.
# Usage: APP_URL=https://... ./scripts/app-shell.sh [-t timeout_seconds] 'command'
set -euo pipefail

TIMEOUT=60
if [[ "${1:-}" == "-t" ]]; then
  TIMEOUT="$2"
  shift 2
fi

APP_URL="${APP_URL:?set APP_URL to the deployed app URL}"
CMD="${1:?usage: app-shell.sh [-t seconds] 'command'}"
TOKEN="$(databricks auth token --output json | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')"

curl -fsS -X POST "${APP_URL}/api/v1/shell/run" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json,sys; print(json.dumps({"argv": ["bash", "-lc", sys.argv[1]], "session_id": "spike-probe", "timeout_seconds": int(sys.argv[2])}))' "$CMD" "$TIMEOUT")" \
  | python3 -c 'import sys,json; r=json.load(sys.stdin); print("[exit=%s %sms]" % (r["exit_code"], r["duration_ms"])); print(r["stdout"], end=""); print(r["stderr"], file=sys.stderr, end="")'
