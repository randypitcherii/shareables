"""Helpers shared by the delegation matrix scripts."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import FQ_SCHEMA, SqlOutcome, run_sql  # noqa: E402


def call_function(w, fn: str, *args: str) -> SqlOutcome:
    """Invoke a function in the experiment schema with string literal args."""
    rendered = ", ".join("'" + a.replace("'", "''") + "'" for a in args)
    return run_sql(w, f"SELECT {FQ_SCHEMA}.{fn}({rendered})")


def decode_json_scalar(outcome: SqlOutcome) -> dict[str, Any]:
    """UDFs return JSON strings so the sandbox's own view of the world survives
    the trip back. Decode it, or explain why it could not be decoded."""
    if not outcome.succeeded:
        return {"_undecodable": "statement failed", "_error": outcome.error}
    try:
        return json.loads(outcome.scalar)
    except (TypeError, ValueError) as exc:
        return {"_undecodable": str(exc), "_raw": str(outcome.scalar)[:400]}
