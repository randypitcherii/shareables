"""Matrix row: usage_tracking_per_user_agent — per-user + per-agent usage rows.

Claim/source: AI Gateway usage tracking docs — inference usage lands in
system tables (system.serving.*) with per-user attribution and client
metadata, enabling per-user/per-agent chargeback and monitoring.

✅ means: the usage system table exists, is queryable with the SSO token via
the SQL Statements API, and exposes BOTH a per-user identity column AND a
user_agent/client-identity column; notes name the exact table and columns.
❓ means: partial proof — e.g. per-user attribution visible but no
user_agent column, the table exists but has no rows yet (ingestion latency
may exceed this script's runtime), or no SQL warehouse was available.
❌ means: no usage system table with per-user attribution exists at all.

Method: make one tiny real inference first with a distinctive User-Agent
header and a `usage_context` payload marker (via _common.chat_completion's
extra_headers/extra_payload), then discover tables via SHOW TABLES IN
system.serving and columns via DESCRIBE, sample rows, and additionally hunt
for the marker itself (ingestion latency may hide it — that's noted, not
failed). Read-only apart from the one tiny inference; no mutations, no gate.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "usage_tracking_per_user_agent"

USER_COL_MARKERS = ("requester", "user", "created_by", "executed_as", "run_as")
# usage_context is the documented per-agent/per-client attribution mechanism
# (a caller-supplied map on the request); user_agent/client_* are candidates.
AGENT_COL_MARKERS = ("user_agent", "client", "agent", "usage_context")


def main() -> int:
    _common.section("09 — Usage tracking: per-user and per-agent attribution")

    model_id = _common.resolve_model_id()
    print(f"  Model: {model_id}")

    # --- Step 1: one tiny real inference so a fresh usage row should exist --
    # Distinctive marker in BOTH the User-Agent header and the documented
    # usage_context payload field, so either attribution mechanism can match.
    marker = f"uag-experiment-row09-{uuid.uuid4().hex[:12]}"
    status, body = _common.chat_completion(
        model_id,
        "Reply with: pong",
        max_tokens=8,
        extra_headers={"User-Agent": marker},
        extra_payload={"usage_context": {"client": marker}},
    )
    print(f"  Fresh inference for attribution: HTTP {status} (marker: {marker})")
    inference_note = f"fresh inference HTTP {status} with marker '{marker}' in User-Agent + usage_context"

    # --- Step 2: discover the usage tables ---------------------------------
    try:
        gov.get_warehouse_id()
    except RuntimeError as exc:
        gov.conclude(ROW_KEY, None, f"No SQL warehouse available — {exc}")
        return 1

    show = gov.sql_exec("SHOW TABLES IN system.serving")
    if not show["ok"]:
        gov.conclude(
            ROW_KEY,
            None,
            "Could not enumerate system.serving tables via the SQL Statements "
            f"API ({show['state']}: {gov.snip(show['error'], 250)}). "
            f"{inference_note}",
        )
        return 1
    tables = [row[1] for row in show["rows"]]
    print(f"  Tables in system.serving: {tables}")

    # Prefer the documented usage table; fall back to anything usage-like.
    candidates = [t for t in tables if "usage" in t.lower()] or tables
    if not candidates:
        gov.conclude(
            ROW_KEY,
            False,
            f"system.serving contains no tables — no usage tracking surface. {inference_note}",
        )
        return 1
    table = f"system.serving.{candidates[0]}"
    print(f"  Probing table: {table}")

    desc = gov.sql_exec(f"DESCRIBE TABLE {table}")
    if not desc["ok"]:
        gov.conclude(
            ROW_KEY,
            None,
            f"{table} exists but DESCRIBE failed ({gov.snip(desc['error'], 250)}). {inference_note}",
        )
        return 1
    columns = [row[0] for row in desc["rows"] if row and row[0] and not row[0].startswith("#")]
    print(f"  Columns: {columns}")

    user_cols = [c for c in columns if any(m in c.lower() for m in USER_COL_MARKERS)]
    agent_cols = [c for c in columns if any(m in c.lower() for m in AGENT_COL_MARKERS)]
    print(f"  Per-user identity columns: {user_cols or 'NONE'}")
    print(f"  User-agent / client columns: {agent_cols or 'NONE'}")

    # --- Step 3: sample rows to prove attribution is POPULATED --------------
    # A column merely existing is not attribution — the values must be there.
    selected = (user_cols + agent_cols)[:6] or [columns[0]]
    sample = gov.sql_exec(f"SELECT {', '.join(selected)} FROM {table} LIMIT 10")
    rows_seen = len(sample["rows"]) if sample["ok"] else 0
    print(f"  Sample query ok={sample['ok']}, rows returned: {rows_seen}")
    if sample["ok"] and sample["rows"]:
        print(f"  Sample (verbatim, truncated): {gov.snip(sample['rows'], 300)}")

    def populated(col: str) -> bool:
        if not (sample["ok"] and sample["rows"] and col in selected):
            return False
        idx = selected.index(col)
        return any(
            row[idx] not in (None, "", "null", "{}") for row in sample["rows"]
        )

    user_populated = [c for c in user_cols if populated(c)]
    agent_populated = [c for c in agent_cols if populated(c)]
    print(f"  Populated per-user columns: {user_populated or 'NONE'}")
    print(f"  Populated agent/client columns: {agent_populated or 'NONE'}")

    # --- Step 3b: hunt for THIS run's marker (best-effort; lag expected) ----
    marker_note = ""
    if agent_cols:
        predicate = " OR ".join(
            f"CAST({c} AS STRING) LIKE '%{marker}%'" for c in agent_cols
        )
        hunt = gov.sql_exec(
            f"SELECT COUNT(*) FROM {table} WHERE {predicate}"
        )
        if hunt["ok"]:
            hits = int(hunt["rows"][0][0]) if hunt["rows"] else 0
            marker_note = (
                f" Marker hunt: {hits} row(s) matched '{marker}'"
                + ("" if hits else " (ingestion latency likely exceeds this run)")
                + "."
            )
            print(f"  Marker rows found: {hits}")
            if hits:
                agent_populated = agent_populated or agent_cols

    detail = (
        f"table={table}; user columns={user_cols} (populated: {user_populated}); "
        f"agent/client columns={agent_cols} (populated: {agent_populated}); "
        f"sampled rows={rows_seen}"
        + ("" if sample["ok"] else f"; sample query error={gov.snip(sample['error'], 150)}")
        + f". {inference_note}.{marker_note}"
    )

    if user_populated and agent_populated:
        _common.ok("Per-user AND per-agent attribution populated in the usage system table.")
        gov.conclude(
            ROW_KEY,
            True,
            "Per-user + agent/client attribution proven with populated values "
            "in the usage system table. Freshness caveat: ingestion latency "
            f"may exceed this run, so the just-made inference row itself was "
            f"not chased. {detail}",
        )
        return 0
    if user_populated:
        gov.conclude(
            ROW_KEY,
            None,
            "Per-user attribution PROVEN (populated), but per-agent attribution "
            "unproven: the agent/client columns exist in the schema yet were "
            "empty/null in every sampled row — callers must send user_agent/"
            f"usage_context metadata for it to populate. {detail}",
        )
        return 0
    if user_cols and rows_seen == 0:
        gov.conclude(
            ROW_KEY,
            None,
            "Schema has the attribution columns but no rows were readable yet "
            "(ingestion latency likely exceeds this script's runtime) — schema "
            f"proven, data unproven. {detail}",
        )
        return 0
    _common.fail("No populated per-user identity column found in the usage table.")
    gov.conclude(ROW_KEY, False, f"No per-user attribution found. {detail}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
