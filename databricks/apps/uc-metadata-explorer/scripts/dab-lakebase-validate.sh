#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
branch_slug="$("${repo_root}/scripts/git-branch-slug.sh" "${1:-}")"

cd "$repo_root"
databricks bundle validate --target dev --var "git_branch_slug=${branch_slug}"
