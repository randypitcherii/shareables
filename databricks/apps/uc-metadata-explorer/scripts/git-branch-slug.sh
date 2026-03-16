#!/usr/bin/env bash
set -euo pipefail

raw_branch="${1:-$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo detached)}"
slug="$(printf '%s' "$raw_branch" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//; s/-+/-/g')"

if [[ -z "$slug" ]]; then
  slug="detached"
fi

if [[ ! "$slug" =~ ^[a-z] ]]; then
  slug="b-${slug}"
fi

# branch_id is dev-<slug>; keep total <= 63 chars per Lakebase constraints.
max_slug_len=59
slug="${slug:0:${max_slug_len}}"
slug="${slug%-}"

if [[ -z "$slug" ]]; then
  slug="detached"
fi

printf '%s\n' "$slug"
