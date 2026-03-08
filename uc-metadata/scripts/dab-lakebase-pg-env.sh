#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
branch_slug="$("${repo_root}/scripts/git-branch-slug.sh" "${1:-}")"

project_id="${LAKEBASE_PROJECT_ID:-uc-metadata}"
endpoint_id="${LAKEBASE_ENDPOINT_ID:-app-rw}"
branch_id="dev-${branch_slug}"
endpoint_name="projects/${project_id}/branches/${branch_id}/endpoints/${endpoint_id}"

endpoint_json="$(databricks postgres get-endpoint "$endpoint_name" -o json)"
user_json="$(databricks current-user me -o json)"

pghost="$(printf '%s' "$endpoint_json" | jq -r '.status.hosts.host // empty')"
pgdatabase="${PGDATABASE_OVERRIDE:-postgres}"
pguser="$(printf '%s' "$user_json" | jq -r '.userName // empty')"

if [[ -z "$pghost" || -z "$pgdatabase" || -z "$pguser" ]]; then
  echo "Unable to resolve one or more PG values from CLI responses." >&2
  echo "endpoint_name=${endpoint_name}" >&2
  exit 1
fi

printf 'export PGHOST=%q\n' "$pghost"
printf 'export PGDATABASE=%q\n' "$pgdatabase"
printf 'export PGUSER=%q\n' "$pguser"
