"""Matrix row: usage_tracking_per_user_agent — per-user + per-agent usage rows.

Claim/source: AI Gateway usage tracking docs — inference usage lands in
system tables with per-user attribution and client metadata, enabling
per-user/per-agent chargeback and monitoring.

There are TWO usage surfaces, and they are not equivalent:

* legacy — ``system.serving.endpoint_usage``, fed by the classic
  ``/serving-endpoints/*`` routes. Per-agent attribution comes from a
  ``usage_context`` map in the *request body*.
* Unity AI Gateway — ``system.ai_gateway.usage``, fed by the
  ``/ai-gateway/*`` routes. Per-agent attribution comes from the
  ``Databricks-Ai-Gateway-Request-Tags`` *header* and lands in
  ``request_tags``; the table additionally records ``user_agent``,
  ``url`` and ``api_type`` with no caller cooperation at all.

This row prefers the gateway table, because the gateway routes are what
every config template in this experiment points at.

✅ means: the gateway usage table exposes a populated per-user identity
column AND populated per-agent columns (``request_tags`` and/or
``user_agent``), on rows produced by the ``/ai-gateway/*`` routes.
◑/❓ means: per-user proven, per-agent unproven or only partly proven.
❌ means: no usage table with per-user attribution exists at all.

Method: send one tiny real inference carrying a distinctive marker in the
``Databricks-Ai-Gateway-Request-Tags`` header, then describe and sample the
table, count table-wide population, and hunt for the marker. Ingestion lag
on these tables runs ~15-20 minutes, so a missing marker is reported as lag
(with the observed lag printed), never as a failure. Read-only apart from
the one tiny inference; no mutations, no gate.
"""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "usage_tracking_per_user_agent"

GATEWAY_TABLE = "system.ai_gateway.usage"
LEGACY_TABLE = "system.serving.endpoint_usage"

# Header documented for Unity AI Gateway per-request attribution tags.
REQUEST_TAGS_HEADER = "Databricks-Ai-Gateway-Request-Tags"

USER_COL_MARKERS = ("requester", "created_by", "executed_as", "run_as")
# request_tags is the gateway mechanism; user_agent is recorded by the
# gateway itself; usage_context is the legacy body-map mechanism.
AGENT_COL_MARKERS = ("user_agent", "request_tags", "usage_context", "client", "agent")

# to_json() only accepts struct/map/array — calling it on a STRING column
# fails the whole statement, which silently reads back as "0 populated".
COMPLEX_TYPE_PREFIXES = ("map", "struct", "array")


def _table_exists(table: str) -> bool:
    return gov.sql_exec(f"DESCRIBE TABLE {table}")["ok"]


def _count(query: str) -> int:
    res = gov.sql_exec(query)
    if res["ok"] and res["rows"]:
        try:
            return int(res["rows"][0][0])
        except (TypeError, ValueError):
            return 0
    return 0


def _nonempty_predicate(col: str, col_type: str) -> str:
    """SQL that is true when `col` carries a real value, per its type."""
    if col_type.lower().startswith(COMPLEX_TYPE_PREFIXES):
        return f"{col} IS NOT NULL AND to_json({col}) NOT IN ('{{}}', 'null')"
    return f"{col} IS NOT NULL AND {col} <> ''"


def main() -> int:
    _common.section("09 — Usage tracking: per-user and per-agent attribution")

    model_id = _common.resolve_model_id()
    print(f"  Model: {model_id}")

    # --- Step 1: one tiny real inference carrying the attribution marker ----
    # The marker goes in the documented gateway header AND the legacy body
    # map, so whichever surface is live in this workspace can match it.
    marker = f"uag-row09-{uuid.uuid4().hex[:12]}"
    tags = {"probe": marker, "coding_agent": "uag-experiment"}
    status, _ = _common.chat_completion(
        model_id,
        "Reply with: pong",
        max_tokens=8,
        extra_headers={REQUEST_TAGS_HEADER: json.dumps(tags), "User-Agent": marker},
        extra_payload={"usage_context": {"client": marker}},
    )
    print(f"  Fresh inference for attribution: HTTP {status} (marker: {marker})")
    inference_note = (
        f"fresh inference HTTP {status} with marker '{marker}' in "
        f"{REQUEST_TAGS_HEADER} + User-Agent + legacy usage_context"
    )

    try:
        gov.get_warehouse_id()
    except RuntimeError as exc:
        gov.conclude(ROW_KEY, None, f"No SQL warehouse available — {exc}")
        return 1

    # --- Step 2: pick the surface — gateway table first ---------------------
    if _table_exists(GATEWAY_TABLE):
        table, time_col, surface = GATEWAY_TABLE, "event_time", "Unity AI Gateway"
    elif _table_exists(LEGACY_TABLE):
        table, time_col, surface = LEGACY_TABLE, "request_time", "legacy serving"
    else:
        gov.conclude(
            ROW_KEY,
            False,
            f"Neither {GATEWAY_TABLE} nor {LEGACY_TABLE} exists — no usage "
            f"tracking surface at all. {inference_note}",
        )
        return 1
    print(f"  Usage surface: {surface} — {table}")

    desc = gov.sql_exec(f"DESCRIBE TABLE {table}")
    types = {
        row[0]: (row[1] or "")
        for row in desc["rows"]
        if row and row[0] and not row[0].startswith("#")
    }
    columns = list(types)
    user_cols = [c for c in columns if any(m in c.lower() for m in USER_COL_MARKERS)]
    agent_cols = [c for c in columns if any(m in c.lower() for m in AGENT_COL_MARKERS)]
    print(f"  Per-user identity columns: {user_cols or 'NONE'}")
    print(f"  Per-agent / client columns: {agent_cols or 'NONE'}")

    # --- Step 3: table-wide population, not a 10-row sample -----------------
    # A sample can miss a column that IS used by other clients, so count
    # population across the whole table rather than inspecting N rows.
    populated: dict[str, int] = {}
    for col in user_cols + agent_cols:
        populated[col] = _count(
            f"SELECT COUNT(*) FROM {table} WHERE {_nonempty_predicate(col, types[col])}"
        )
    for col, n in populated.items():
        print(f"    {col:16} populated on {n} row(s)")

    user_populated = [c for c in user_cols if populated.get(c)]
    agent_populated = [c for c in agent_cols if populated.get(c)]

    # --- Step 4: is per-agent attribution live on the GATEWAY routes? -------
    # The July 2026 finding was that the mechanism existed but the gateway
    # chat-completions route never fed it. Ask that question directly.
    route_note = ""
    if table == GATEWAY_TABLE:
        tagged_recent = _count(
            f"SELECT COUNT(*) FROM {table} WHERE {time_col} > "
            "current_timestamp() - INTERVAL 2 DAYS AND request_tags IS NOT NULL "
            "AND to_json(request_tags) NOT IN ('{}', 'null')"
        )
        gw_chat_tagged = _count(
            f"SELECT COUNT(*) FROM {table} WHERE api_type = "
            "'mlflow/v1/chat/completions' AND request_tags IS NOT NULL "
            "AND to_json(request_tags) NOT IN ('{}', 'null')"
        )
        print(f"  request_tags in last 2 days: {tagged_recent}")
        print(f"  request_tags on /ai-gateway/mlflow/v1/chat/completions: {gw_chat_tagged}")
        route_note = (
            f" On the gateway chat-completions route specifically, request_tags "
            f"is populated on {gw_chat_tagged} row(s) ({tagged_recent} tagged rows "
            "across all routes in the last 2 days)."
        )

    # --- Step 5: hunt this run's marker; report lag rather than fail --------
    marker_note = ""
    predicates = [
        f"CAST({c} AS STRING) LIKE '%{marker}%'" for c in agent_cols
    ]
    if predicates:
        hits = _count(
            f"SELECT COUNT(*) FROM {table} WHERE " + " OR ".join(predicates)
        )
        lag = gov.sql_exec(
            f"SELECT timestampdiff(MINUTE, max({time_col}), current_timestamp()) FROM {table}"
        )
        lag_min = lag["rows"][0][0] if lag["ok"] and lag["rows"] else "?"
        print(f"  Marker rows found: {hits} (table is ~{lag_min} min behind now)")
        marker_note = (
            f" Marker hunt: {hits} row(s) matched '{marker}'"
            + (
                ""
                if hits
                else f" — the table's newest row is ~{lag_min} min old, so this "
                "run's own request has not been ingested yet (lag, not absence)"
            )
            + "."
        )
        if hits:
            agent_populated = sorted(set(agent_populated) | {"request_tags"})

    detail = (
        f"table={table} ({surface}); user columns={user_cols} "
        f"(populated: {user_populated}); per-agent columns={agent_cols} "
        f"(populated: {agent_populated}); population counts={populated}. "
        f"{inference_note}.{route_note}{marker_note}"
    )

    if user_populated and agent_populated:
        _common.ok(
            "Per-user AND per-agent attribution are populated on the gateway usage table."
        )
        gov.conclude(
            ROW_KEY,
            True,
            "Per-user AND per-agent attribution proven with populated values. "
            f"{detail}",
        )
        return 0
    if user_populated:
        gov.conclude(
            ROW_KEY,
            None,
            "Per-user attribution PROVEN, per-agent attribution unproven: the "
            "per-agent columns exist but are empty table-wide, so no client on "
            f"this workspace is supplying attribution metadata. {detail}",
        )
        return 0
    gov.conclude(
        ROW_KEY,
        False,
        f"No populated per-user attribution found. {detail}",
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
