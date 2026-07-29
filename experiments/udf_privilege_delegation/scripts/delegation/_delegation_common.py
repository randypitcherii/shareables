"""Helpers shared by the delegation matrix scripts."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import FQ_SCHEMA, SqlOutcome, run_sql  # noqa: E402

# Unity Catalog refuses a connection the caller lacks USE CONNECTION on by
# returning a 403 *inside* an otherwise successful statement, in the struct
# http_request() hands back — not by failing the statement. Scoring on
# `outcome.succeeded` therefore reads an authorization refusal as a working
# call, which is trap 3 wearing a different hat: an HTTP-shaped answer is not
# proof that the thing the HTTP status describes was allowed.
_DENIAL_MARKERS = ("PERMISSION_DENIED", "USE CONNECTION", "INSUFFICIENT_PERMISSIONS")


def http_request_outcome(w, sql: str) -> dict[str, Any]:
    """Run a statement whose single column is an http_request() struct, and
    classify the result as allowed, denied-by-Unity-Catalog, or statement error.

    ``sql`` must select the struct itself (``SELECT http_request(...) AS r``) so
    both halves of the answer — the status code and the body carrying the
    refusal — survive to the classifier.
    """
    outcome = run_sql(w, sql)
    if not outcome.succeeded:
        # A statement-level refusal is still a refusal; UC raises this shape when
        # the object cannot be resolved at all rather than when the call is denied.
        denied = any(m in (outcome.error or "") for m in _DENIAL_MARKERS)
        return {
            "statement_succeeded": False,
            "status_code": None,
            "body": None,
            "error_code": outcome.error_code,
            "error": outcome.error,
            "denied_by_unity_catalog": denied,
            "allowed": False,
        }

    try:
        payload = json.loads(outcome.scalar)
        status_code = str(payload.get("status_code"))
        body = str(payload.get("text", ""))
    except (TypeError, ValueError):
        status_code, body = None, str(outcome.scalar)

    denied = any(m in body for m in _DENIAL_MARKERS)
    return {
        "statement_succeeded": True,
        "status_code": status_code,
        "body": _clip(body),
        "error_code": None,
        "error": None,
        "denied_by_unity_catalog": denied,
        # Allowed means the platform let the request out and the origin answered.
        "allowed": not denied and status_code == "200",
    }


def _clip(text: str, limit: int = 500) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[:limit] + " ...[truncated]"


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
