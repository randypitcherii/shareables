"""Matrix row: per_user_spend_alerting — AI Gateway per-user usage/alerting.

Claim/source: Databricks AI Gateway docs — endpoints support usage tracking
(per-user attribution into system tables) and per-user rate limits, which
together are the supported path to per-user spend ALERTING (system-table
queries + SQL alerts / budget policies). Believed ✅.

✅ means: the per-user alerting configuration surface exists on this
endpoint — usage tracking (per-user attribution) is enabled/settable, and
the ai_gateway config exposes per-user knobs; notes list the exact fields.
❌ means: no usage-tracking / per-user config surface exists at all.
❓ means: the endpoint's ai_gateway config could not be read and (without
ALLOW_ENDPOINT_MUTATION=1) settability could not be probed.

This is a configuration-surface probe ONLY — it never generates spend.
Read-only by default; with ALLOW_ENDPOINT_MUTATION=1 it will additionally
attempt to enable usage tracking via PUT .../ai-gateway and restore the
original config in a finally block.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "per_user_spend_alerting"


def main() -> int:
    _common.section("07 — Per-user spend alerting config surface (AI Gateway)")

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
            f"{gov.snip(body, 200)}) — ai_gateway alerting surface unobservable",
        )
        return 1

    ai_gateway = body.get("ai_gateway", {}) or {}
    print(f"  ai_gateway config present: {bool(ai_gateway)}")
    if ai_gateway:
        print(f"  ai_gateway keys: {sorted(ai_gateway.keys())}")
        print(f"  ai_gateway (verbatim): {json.dumps(ai_gateway, indent=2, sort_keys=True)}")

    usage_cfg = ai_gateway.get("usage_tracking_config") or {}
    usage_enabled = bool(usage_cfg.get("enabled"))
    rate_limits = ai_gateway.get("rate_limits") or []
    per_user_limits = [rl for rl in rate_limits if rl.get("key") == "user"]

    field_report = (
        f"ai_gateway keys={sorted(ai_gateway.keys()) if ai_gateway else '(absent)'}; "
        f"usage_tracking_config={gov.snip(usage_cfg, 150) or '(absent)'}; "
        f"rate_limits={gov.snip(rate_limits, 200) or '(absent)'}; "
        f"per-user rate limits={len(per_user_limits)}"
    )

    if usage_enabled:
        _common.ok("usage_tracking_config.enabled=true — per-user attribution flows to system tables.")
        gov.conclude(
            ROW_KEY,
            True,
            "Per-user alerting surface present: usage tracking is ON (per-user "
            "attribution into system.serving usage tables — the documented "
            "basis for spend alerts via SQL alerts/budget policies). Note: no "
            "direct 'alert at $X per user' field exists ON the endpoint; "
            f"alerting is composed from these knobs. {field_report}",
        )
        return 0

    # Usage tracking not enabled (or ai_gateway absent). Settability probe is
    # a mutation — gate it.
    if not gov.mutation_allowed():
        gov.print_mutation_gate()
        print("  The mutation path would attempt (then restore):")
        print(f"    PUT /api/2.0/serving-endpoints/{endpoint_name}/ai-gateway")
        print('    {"usage_tracking_config": {"enabled": true}}')
        gov.conclude(
            ROW_KEY,
            None,
            "usage tracking not currently enabled on this endpoint and "
            f"{gov.MUTATION_ENV}=1 not set, so settability was not probed. "
            f"{field_report}",
        )
        return 0

    original_gateway = ai_gateway
    mutated = False
    try:
        put_status, put_body = gov.api_request(
            "PUT",
            f"/api/2.0/serving-endpoints/{endpoint_name}/ai-gateway",
            payload={"usage_tracking_config": {"enabled": True}},
        )
        mutated = put_status == 200
        print(f"  PUT ai-gateway usage_tracking enabled -> HTTP {put_status}")
        if put_status == 200:
            _common.ok("Usage tracking accepted configuration — alerting surface is settable.")
            gov.conclude(
                ROW_KEY,
                True,
                "usage_tracking_config accepted enabled=true via PUT ai-gateway "
                f"(restored afterwards). {field_report}",
            )
            return 0
        _common.fail("The endpoint refused usage-tracking configuration.")
        gov.conclude(
            ROW_KEY,
            False,
            f"PUT ai-gateway usage_tracking -> HTTP {put_status} "
            f"({gov.snip(put_body, 200)}) — no per-user alerting surface on "
            f"this endpoint. {field_report}",
        )
        return 1
    finally:
        if mutated:
            print("  Cleanup: restoring the original ai_gateway config...")
            restore_status, restore_body = gov.api_request(
                "PUT",
                f"/api/2.0/serving-endpoints/{endpoint_name}/ai-gateway",
                payload=original_gateway,
            )
            if restore_status == 200:
                _common.ok("ai_gateway config restored.")
            else:
                _common.fail(
                    f"ai_gateway restore failed (HTTP {restore_status}): "
                    f"{gov.snip(restore_body, 150)} — restore manually in the UI."
                )


if __name__ == "__main__":
    sys.exit(main())
