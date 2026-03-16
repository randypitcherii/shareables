#!/usr/bin/env python3
"""Build slug-safe dev resource names for Databricks + Lakebase constraints."""

from __future__ import annotations

import os
import re
import sys

_TOKEN_PATTERN = re.compile(r"[^a-z0-9]+")
_PROJECT_ID_PATTERN = re.compile(r"^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")
_MAX_LEN = 63


def _slugify_token(value: str, *, default: str) -> str:
    token = _TOKEN_PATTERN.sub("-", value.lower()).strip("-")
    token = re.sub(r"-+", "-", token)
    if not token:
        token = default
    if not token[0].isalpha():
        token = f"u-{token}"
    token = token.rstrip("-")
    return token or default


def build_dev_resource_name(
    base_name: str,
    raw_user: str | None = None,
    *,
    max_len: int = _MAX_LEN,
) -> str:
    """Compose `<user>-<base>` that satisfies slug + length constraints."""
    if max_len < 3:
        raise ValueError("max_len must be at least 3")
    user = raw_user or os.environ.get("DATABRICKS_USER") or os.environ.get("USER") or "user"
    user_slug = _slugify_token(user, default="user")
    base_slug = _slugify_token(base_name, default="app")

    max_user_len = max_len - len(base_slug) - 1
    if max_user_len < 1:
        candidate = base_slug[:max_len].rstrip("-")
        if not candidate or not candidate[0].isalpha():
            candidate = f"a-{candidate}"[:max_len].rstrip("-")
        return candidate

    user_slug = user_slug[:max_user_len].rstrip("-")
    if not user_slug:
        user_slug = "u"

    candidate = f"{user_slug}-{base_slug}"
    candidate = candidate[:max_len].rstrip("-")
    if not _PROJECT_ID_PATTERN.fullmatch(candidate):
        raise ValueError(f"generated invalid project id: {candidate!r}")
    return candidate


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: dev_resource_name.py <base-name> [raw-user] [max-len]", file=sys.stderr)
        return 2
    base_name = sys.argv[1]
    raw_user = sys.argv[2] if len(sys.argv) > 2 else None
    max_len = int(sys.argv[3]) if len(sys.argv) > 3 else _MAX_LEN
    print(build_dev_resource_name(base_name, raw_user, max_len=max_len))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
