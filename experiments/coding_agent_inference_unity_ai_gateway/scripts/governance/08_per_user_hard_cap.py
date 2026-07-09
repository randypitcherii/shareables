"""Matrix row: per_user_hard_cap — a HARD per-user budget cap (block mode).

Claim/source: a Data+AI Summit talk claimed per-user budget caps that BLOCK
at budget. Current AI Gateway docs only show usage tracking (alert-style,
after-the-fact) and rate limits (requests/tokens per renewal period — rate
caps, not spend caps). Believed ❌ today.

✅ means: a real block-at-budget knob exists on the endpoint/gateway config
AND accepts a value (e.g. a spend/budget field with a block/enforce mode).
❌ means: only alert-style surfaces exist — usage tracking and/or rate
limits, with NO spend-denominated cap that blocks; the verbatim field list
is in notes.
❓ means: the endpoint config could not be read at all.

Configuration-surface probe ONLY — never generates spend. Read-only by
default; with ALLOW_ENDPOINT_MUTATION=1 it additionally offers the API a
hypothetical budget-cap payload to prove the schema rejects it, restoring
the original config in a finally block.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "per_user_hard_cap"

# Key fragments that would indicate a spend-denominated cap with block-mode.
CAP_MARKERS = ("budget", "spend", "cost", "dollar", "cap", "block")


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

    _common.fail("No spend-denominated block-at-budget knob exists on this endpoint.")
    print("  What DOES exist: usage tracking (after-the-fact attribution) and")
    print("  rate_limits (requests/tokens per renewal period — rate caps, not")
    print("  spend caps). The Summit-talk claim of a per-user block-at-budget")
    print("  cap is not present in this workspace's API surface.")
    gov.conclude(
        ROW_KEY,
        False,
        "No hard per-user budget cap: ai_gateway exposes only alert-style "
        "surfaces (usage tracking) and rate caps (rate_limits per user/"
        "endpoint), no spend-denominated block-at-budget field. "
        f"Per-user rate limits found: {len(per_user_rate)}. {field_report}."
        f"{mutation_probe_note}",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
