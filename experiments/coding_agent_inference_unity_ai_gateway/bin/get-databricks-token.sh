#!/bin/sh
# get-databricks-token.sh — mint a short-lived Databricks OAuth access token.
#
# SSO/OAuth ONLY. This helper never emits, accepts, or falls back to a static
# personal access token (dapi...). If the resolved credential looks like a PAT,
# it refuses.
#
# Supports both auth styles, still zero-PAT:
#   * U2M (user OAuth)  — `databricks auth token` (the default `databricks auth login` flow)
#   * M2M (service principal) — profiles with client_id/client_secret mint a
#     token from the workspace OIDC endpoint (client-credentials grant). This
#     is how the governance rows run as a second test principal.
#
# Inputs (environment):
#   DATABRICKS_CONFIG_PROFILE  CLI profile to mint the token with (default: DEFAULT)
#   DATABRICKS_CONFIG_FILE     optional config path (default: ~/.databrickscfg)
#   DATABRICKS_HOST            optional workspace URL; validated against the profile
#
# Output: the access token, alone, on stdout. All diagnostics go to stderr.

set -eu

PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
CONFIG_FILE="${DATABRICKS_CONFIG_FILE:-$HOME/.databrickscfg}"

# --- Service-principal (OAuth M2M) profiles -------------------------------
# `databricks auth token` only supports U2M (user) auth. If the profile has
# client_id/client_secret, mint the token via the workspace OIDC
# client-credentials endpoint instead. Still OAuth, still zero PATs.
_cfg_get() {
    # _cfg_get <profile> <key> — read one key from the profile's section.
    awk -v section="[$1]" -v key="$2" '
        $0 == section { in_section = 1; next }
        /^\[/         { in_section = 0 }
        in_section && $1 == key { print $3; exit }
    ' "$CONFIG_FILE" 2>/dev/null
}

CLIENT_ID=$(_cfg_get "$PROFILE" "client_id" || true)
if [ -n "${CLIENT_ID:-}" ]; then
    CLIENT_SECRET=$(_cfg_get "$PROFILE" "client_secret" || true)
    M2M_HOST="${DATABRICKS_HOST:-$(_cfg_get "$PROFILE" "host" || true)}"
    if [ -z "${CLIENT_SECRET:-}" ] || [ -z "${M2M_HOST:-}" ]; then
        echo "error: profile '${PROFILE}' has client_id but is missing client_secret or host." >&2
        exit 1
    fi
    output=$(curl -sf -X POST "${M2M_HOST%/}/oidc/v1/token" \
        -u "${CLIENT_ID}:${CLIENT_SECRET}" \
        -d "grant_type=client_credentials&scope=all-apis") || {
        echo "error: OAuth M2M token request failed for profile '${PROFILE}' (host ${M2M_HOST})." >&2
        echo "Check the service principal's client_id/client_secret and workspace access." >&2
        exit 1
    }
    token=$(printf '%s\n' "$output" \
        | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
        | head -n 1)
    if [ -z "$token" ]; then
        echo "error: OIDC token endpoint returned no access_token for profile '${PROFILE}'." >&2
        exit 1
    fi
    printf '%s\n' "$token"
    exit 0
fi

if ! command -v databricks >/dev/null 2>&1; then
    echo "error: 'databricks' CLI not found on PATH." >&2
    echo "Install it (https://docs.databricks.com/dev-tools/cli/install.html), then authenticate with:" >&2
    echo "  databricks auth login --host https://<your-workspace> --profile ${PROFILE}" >&2
    exit 1
fi

if [ -n "${DATABRICKS_HOST:-}" ]; then
    output=$(databricks auth token --profile "$PROFILE" --host "$DATABRICKS_HOST" -o json 2>&1) || {
        status=$?
        echo "error: could not mint an OAuth token for profile '${PROFILE}' (host ${DATABRICKS_HOST})." >&2
        echo "$output" >&2
        echo "Authenticate with: databricks auth login --host ${DATABRICKS_HOST} --profile ${PROFILE}" >&2
        exit "$status"
    }
else
    output=$(databricks auth token --profile "$PROFILE" -o json 2>&1) || {
        status=$?
        echo "error: could not mint an OAuth token for profile '${PROFILE}'." >&2
        echo "$output" >&2
        echo "Authenticate with: databricks auth login --host https://<your-workspace> --profile ${PROFILE}" >&2
        exit "$status"
    }
fi

# Extract the access_token field from the JSON output (no jq dependency).
token=$(printf '%s\n' "$output" \
    | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
    | head -n 1)

if [ -z "$token" ]; then
    echo "error: 'databricks auth token' returned no access_token for profile '${PROFILE}'." >&2
    echo "$output" >&2
    echo "This usually means the profile is not OAuth-authenticated. Run:" >&2
    echo "  databricks auth login --host https://<your-workspace> --profile ${PROFILE}" >&2
    exit 1
fi

case "$token" in
    dapi*)
        echo "error: resolved credential looks like a static personal access token (dapi...)." >&2
        echo "This experiment is SSO-only — PATs are refused by design. Use OAuth instead:" >&2
        echo "  databricks auth login --host https://<your-workspace> --profile ${PROFILE}" >&2
        exit 1
        ;;
esac

printf '%s\n' "$token"
