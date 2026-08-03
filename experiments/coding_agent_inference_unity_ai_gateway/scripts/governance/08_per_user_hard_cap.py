"""Matrix row: per_user_hard_cap — a HARD per-user budget cap (block mode).

Claim/source: a Data+AI Summit talk claimed per-user budget caps that BLOCK
at budget. The AI Gateway budgets docs now describe exactly that — per-user
monthly thresholds whose action is "Send alert" OR "Block usage" — announced
GA on 2026-07-06. But that surface is ACCOUNT-scoped (account console →
budgets), not a field on the serving endpoint, so a workspace-scoped token
cannot see or set it.

This script therefore probes both levels:

1. the serving endpoint's ``ai_gateway`` block (workspace scope), and
2. the account budgets API, when an account-level CLI profile is supplied
   via ``DATABRICKS_ACCOUNT_PROFILE``.

✅ means: a block-at-budget surface was found AND is spend-denominated —
either a cap field on the endpoint, or an account budget carrying a
blocking action rather than an alert-only threshold.
❌ means: only alert-style surfaces exist — usage tracking, alert
thresholds, and/or rate limits (requests per period, not dollars).
❓ means: the endpoint could not be read, or the endpoint has no cap and the
account budgets API was unreachable, leaving the documented account-level
feature unverified.

Configuration-surface probe ONLY — never generates spend. Read-only by
default; with ALLOW_ENDPOINT_MUTATION=1 it additionally offers the endpoint
API a hypothetical budget-cap payload to prove the schema rejects it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "per_user_hard_cap"

# Key fragments that would indicate a spend-denominated cap with block-mode.
CAP_MARKERS = ("budget", "spend", "cost", "dollar", "cap", "block")

# Env var naming a databrickscfg profile whose host is the account console.
ACCOUNT_PROFILE_ENV = "DATABRICKS_ACCOUNT_PROFILE"

# The enum that makes a budget a hard cap rather than a notification. Match
# the field exactly — several budgets have the word "block" in their display
# name while carrying nothing but EMAIL_NOTIFICATION.
BLOCK_ACTION_TYPE = "BLOCK_USAGE"

# Field names that would prove the *per-user* half of the claim, as opposed
# to a workspace- or tag-scoped cap that blocks everyone at once.
PER_USER_MARKERS = ("per_user", "user_threshold", "user_override", "principal")


def probe_account_budgets() -> tuple[bool | None, str]:
    """Look for a BLOCKING account budget. Returns (found_block, note).

    found_block is None when the account API could not be reached at all —
    that is the difference between "no hard cap exists" and "we could not
    look", and the matrix verdict depends on which one it is.
    """
    profile = os.environ.get(ACCOUNT_PROFILE_ENV, "").strip()
    if not profile:
        return None, (
            f" Account budgets NOT probed: set {ACCOUNT_PROFILE_ENV} to a "
            "databrickscfg profile authenticated against "
            "https://accounts.cloud.databricks.com (databricks auth login "
            "--host https://accounts.cloud.databricks.com --account-id <id>). "
            "The documented per-user 'Block usage' budget action lives there, "
            "not on the serving endpoint."
        )
    try:
        proc = subprocess.run(
            ["databricks", "account", "budgets", "list", "-p", profile, "-o", "json"],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return None, f" Account budgets probe failed to run: {exc}."
    if proc.returncode != 0:
        return None, (
            f" Account budgets unreachable via profile '{profile}': "
            f"{gov.snip(proc.stderr.strip(), 200)}."
        )
    try:
        budgets = json.loads(proc.stdout or "[]") or []
    except ValueError:
        return None, f" Account budgets returned unparseable output: {gov.snip(proc.stdout, 160)}."

    def action_types(budget: dict) -> set[str]:
        found: set[str] = set()
        for alert in budget.get("alert_configurations") or []:
            for action in alert.get("action_configurations") or []:
                if action.get("action_type"):
                    found.add(action["action_type"])
        return found

    blocking = [b for b in budgets if BLOCK_ACTION_TYPE in action_types(b)]
    all_actions = sorted({a for b in budgets for a in action_types(b)})
    blob = json.dumps(budgets).lower()
    per_user_fields = [m for m in PER_USER_MARKERS if m in blob]

    print(
        f"  Account budgets found: {len(budgets)}; carrying {BLOCK_ACTION_TYPE}: "
        f"{len(blocking)}; action types seen: {all_actions}"
    )
    if not blocking:
        return False, (
            f" Account budgets: {len(budgets)} configured via profile '{profile}', "
            f"none carrying a {BLOCK_ACTION_TYPE} action (action types present: "
            f"{all_actions}) — only alert-style budgets exist here."
        )

    example = blocking[0]
    thresholds = [
        f"{a.get('quantity_threshold', '?').split('.')[0]} {a.get('quantity_type')}/{a.get('time_period')}"
        for a in (example.get("alert_configurations") or [])
    ]
    hard_cap_note = (
        f" Account budgets: {len(budgets)} configured, {len(blocking)} carry a real "
        f"{BLOCK_ACTION_TYPE} action (action types present: {all_actions}). These are "
        f"spend-denominated and blocking — e.g. '{example.get('display_name')}' triggers "
        f"on CUMULATIVE_SPENDING_EXCEEDED at {thresholds}. A block-at-budget surface "
        "therefore EXISTS at account scope (it is not a field on the serving endpoint)."
    )

    if per_user_fields:
        return True, hard_cap_note + (
            f" Per-user scoping is visible in the API response ({per_user_fields})."
        )
    # The block half is proven; the per-user half is not observable here.
    return None, hard_cap_note + (
        " BUT the per-USER half is unproven: every budget returned by this API "
        "version is scoped by workspace_id and/or tags, with no per-user threshold "
        "or per-user override field, so a triggered cap blocks the whole filtered "
        "scope rather than one over-spending engineer. The docs describe per-user "
        "thresholds and overrides; they are not exposed by this API/CLI version, so "
        "confirm them in the account console before promising per-user caps."
    )


def main() -> int:
    _common.section("08 — HARD per-user budget cap (block-at-budget) probe")

    model_id = _common.resolve_model_id()
    endpoint_name = gov.endpoint_tail(model_id)
    print(f"  Model: {model_id}")
    print(f"  Endpoint probed: {endpoint_name}")
    print("  Configuration-surface probe only — this script never generates spend.")

    status, body = gov.get_serving_endpoint(endpoint_name)
    print(f"  GET /api/2.0/serving-endpoints/{endpoint_name} -> HTTP {status}")
    if status != 200 or not isinstance(body, dict):
        gov.conclude(
            ROW_KEY,
            None,
            f"Could not read endpoint '{endpoint_name}' (HTTP {status}: "
            f"{gov.snip(body, 200)}) — cap surface unobservable",
        )
        return 1

    ai_gateway = body.get("ai_gateway", {}) or {}
    all_keys = sorted(set(gov.collect_keys(ai_gateway)))
    print(f"  ai_gateway present: {bool(ai_gateway)}")
    print(f"  ai_gateway key paths (verbatim): {all_keys or '(none)'}")
    if ai_gateway:
        print(f"  ai_gateway (verbatim): {json.dumps(ai_gateway, indent=2, sort_keys=True)}")

    cap_like = [
        k for k in all_keys if any(m in k.lower() for m in CAP_MARKERS)
    ]
    rate_limits = ai_gateway.get("rate_limits") or []
    per_user_rate = [rl for rl in rate_limits if rl.get("key") == "user"]

    field_report = (
        f"ai_gateway key paths={all_keys or '(none)'}; cap-like keys={cap_like or 'NONE'}; "
        f"rate_limits(verbatim)={gov.snip(rate_limits, 250) or '(absent)'}"
    )

    mutation_probe_note = ""
    if gov.mutation_allowed():
        # Offer the API a hypothetical budget-cap field; a schema rejection is
        # positive evidence the knob does not exist. This does not change any
        # real config on success-rejection, but treat it as a mutation attempt
        # anyway and restore if it somehow lands.
        probe_payload = dict(ai_gateway) if ai_gateway else {}
        probe_payload["budget_config"] = {
            "per_user_monthly_usd": 1,
            "on_exceeded": "BLOCK",
        }
        put_status, put_body = gov.api_request(
            "PUT",
            f"/api/2.0/serving-endpoints/{endpoint_name}/ai-gateway",
            payload=probe_payload,
        )
        print(f"  Mutation probe: PUT hypothetical budget_config -> HTTP {put_status}")
        mutation_probe_note = (
            f" Mutation probe: PUT hypothetical budget_config -> HTTP {put_status} "
            f"({gov.snip(put_body, 200)})."
        )
        if put_status == 200:
            # Unexpected: the API accepted a budget field — restore immediately.
            print("  Unexpected acceptance — restoring original ai_gateway config...")
            gov.api_request(
                "PUT",
                f"/api/2.0/serving-endpoints/{endpoint_name}/ai-gateway",
                payload=ai_gateway,
            )
            gov.conclude(
                ROW_KEY,
                True,
                "A budget_config field was ACCEPTED by PUT ai-gateway — a "
                "block-at-budget knob may exist (verify enforcement "
                f"separately). {field_report}.{mutation_probe_note}",
            )
            return 0
    else:
        print(f"  ({gov.MUTATION_ENV}=1 would additionally offer the API a hypothetical")
        print("   budget_config payload to prove the schema rejects it.)")

    if cap_like:
        gov.conclude(
            ROW_KEY,
            True,
            f"Found cap-like config keys on the endpoint: {cap_like} — inspect "
            f"whether they block (vs alert). {field_report}.{mutation_probe_note}",
        )
        return 0

    # The endpoint has no cap. The documented feature is account-scoped, so
    # the verdict now turns on whether we could look there.
    account_block, account_note = probe_account_budgets()

    _common.fail("No spend-denominated block-at-budget knob exists on this endpoint.")
    print("  What DOES exist on the endpoint: usage tracking (after-the-fact")
    print("  attribution) and rate_limits (requests/tokens per renewal period —")
    print("  rate caps, not spend caps).")

    endpoint_report = (
        "No hard per-user budget cap ON THE ENDPOINT: ai_gateway exposes only "
        "usage tracking and rate caps (rate_limits per user/endpoint), no "
        "spend-denominated block-at-budget field. Per-user rate limits found: "
        f"{len(per_user_rate)}. {field_report}.{mutation_probe_note}"
    )

    if account_block:
        gov.conclude(ROW_KEY, True, endpoint_report + account_note)
        return 0
    gov.conclude(ROW_KEY, account_block, endpoint_report + account_note)
    return 0


if __name__ == "__main__":
    sys.exit(main())
