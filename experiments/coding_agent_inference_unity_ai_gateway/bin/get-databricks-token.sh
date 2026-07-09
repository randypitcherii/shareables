#!/bin/sh
# get-databricks-token.sh — mint a short-lived Databricks OAuth access token.
#
# SSO/OAuth ONLY. This helper never emits, accepts, or falls back to a static
# personal access token (dapi...). If the resolved credential looks like a PAT,
# it refuses.
#
# Inputs (environment):
#   DATABRICKS_CONFIG_PROFILE  CLI profile to mint the token with (default: DEFAULT)
#   DATABRICKS_HOST            optional workspace URL; validated against the profile
#
# Output: the access token, alone, on stdout. All diagnostics go to stderr.

set -eu

PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"

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
