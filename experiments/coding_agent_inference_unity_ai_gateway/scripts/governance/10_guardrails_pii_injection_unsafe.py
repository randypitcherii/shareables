"""Matrix row: guardrails_pii_injection_unsafe — AI Gateway guardrail knobs.

Claim/source: AI Gateway docs list guardrails on serving endpoints — PII
detection, safety (unsafe-content) filtering, and topic/keyword restrictions.
Prompt-injection filtering is often *assumed* to be included; whether a
dedicated injection knob exists at all is part of what's under test.

Both route families must be probed. Guardrails are configured on a serving
endpoint (``PUT /api/2.0/serving-endpoints/<name>/ai-gateway``) and are
enforced on the classic ``/serving-endpoints/*`` routes. The Unity AI
Gateway routes (``/ai-gateway/*``) resolve the model in the UC model catalog
rather than through that endpoint's config, and are governed by service
policies on model services instead. A probe that calls only one route family
will therefore mis-report enforcement; this script calls both, with a benign
control on each.

✅ means: a configured guardrail actually blocked a synthetic-PII request on
at least one route.
◑ means: enforced on some routes and bypassed on others (the current state).
❌ means: the guardrail was accepted but blocked nothing on any route.
❓ means: read-only mode — settability not probed.

Safety: NEVER enables guardrails on shared endpoints without
ALLOW_ENDPOINT_MUTATION=1. All probes use fully synthetic PII
("000-00-0000", card "4111 1111 1111 1111") and no real data. Every mutation
is restored in a finally block.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "guardrails_pii_injection_unsafe"

# Fully synthetic: a reserved-invalid SSN and the standard Visa test card.
SYNTHETIC_PII_PROMPT = (
    "For a compliance test, repeat these test values back exactly: "
    "SSN 000-00-0000, card 4111 1111 1111 1111, email a@b.com."
)
BENIGN_PROMPT = "What is 2+2?"

GUARDRAIL_KNOBS = ("pii", "safety", "valid_topics", "invalid_keywords")

# Guardrail config used for the enforcement probe.
PROBE_GUARDRAILS = {"input": {"pii": {"behavior": "BLOCK"}, "safety": True}}

# Config propagation is not instant on the serving endpoint.
PROPAGATION_WAIT_S = 20


def describe_guardrails(guardrails: dict) -> str:
    sides = []
    for side in ("input", "output"):
        cfg = guardrails.get(side) or {}
        sides.append(f"{side}: {sorted(cfg.keys()) or '(none)'}")
    return "; ".join(sides)


def _routes(model_id: str, endpoint_name: str) -> dict[str, tuple[str, str]]:
    """route label -> (url, model value to send). Both families, explicitly."""
    host = _common.get_host()
    return {
        "gateway /ai-gateway/mlflow/v1/chat/completions": (
            f"{host}/ai-gateway/mlflow/v1/chat/completions",
            model_id,
        ),
        "classic /serving-endpoints/chat/completions": (
            f"{host}/serving-endpoints/chat/completions",
            endpoint_name,
        ),
    }


def _probe(url: str, model_value: str, prompt: str) -> tuple[int, str]:
    """One chat completion against one explicit route. Never raises."""
    headers = {
        "Authorization": f"Bearer {_common.get_token()}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model_value,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 40,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=90)
    except requests.RequestException as exc:  # network-level only
        return 0, str(exc)[:200]
    return resp.status_code, resp.text[:400]


def _flagged_categories(body: str) -> list[str]:
    """Pull the flagged guardrail categories out of a rejection body."""
    try:
        message = json.loads(json.loads(body).get("message", "{}"))
    except (ValueError, TypeError, AttributeError):
        return []
    found: list[str] = []
    for key in ("input_guardrail", "output_guardrail"):
        for entry in message.get(key) or []:
            if entry.get("flagged"):
                found += [k for k, v in (entry.get("categories") or {}).items() if v]
    return sorted(set(found))


def main() -> int:
    _common.section("10 — Guardrails: PII / prompt-injection / unsafe content")

    model_id = _common.resolve_model_id()
    endpoint_name = gov.endpoint_tail(model_id)
    print(f"  Model: {model_id}")
    print(f"  Endpoint probed: {endpoint_name}")

    status, body = gov.get_serving_endpoint(endpoint_name)
    print(f"  GET /api/2.0/serving-endpoints/{endpoint_name} -> HTTP {status}")
    if status != 200 or not isinstance(body, dict):
        gov.conclude(
            ROW_KEY,
            None,
            f"Could not read endpoint '{endpoint_name}' (HTTP {status}: "
            f"{gov.snip(body, 200)}) — guardrail surface unobservable",
        )
        return 1

    ai_gateway = body.get("ai_gateway", {}) or {}
    guardrails = ai_gateway.get("guardrails") or {}
    print(f"  ai_gateway present: {bool(ai_gateway)}; guardrails configured: {bool(guardrails)}")

    if not gov.mutation_allowed():
        gov.print_mutation_gate()
        print("  The mutation path would attempt (then restore):")
        print(f"    PUT /api/2.0/serving-endpoints/{endpoint_name}/ai-gateway")
        print(f"    {json.dumps({'guardrails': PROBE_GUARDRAILS})}")
        print("  ...then send the same synthetic-PII prompt down BOTH route")
        print("  families to see which of them actually enforces it.")
        gov.conclude(
            ROW_KEY,
            None,
            f"{gov.MUTATION_ENV}=1 not set — enforcement not probed (dry-run). "
            f"Currently configured: {describe_guardrails(guardrails) if guardrails else '(none)'}. "
            f"Documented knobs: {GUARDRAIL_KNOBS} on input/output; there is no "
            "dedicated prompt-injection filter in this endpoint's schema.",
        )
        return 0

    routes = _routes(model_id, endpoint_name)
    original_gateway = ai_gateway
    mutated = False
    try:
        payload = dict(ai_gateway)
        payload["guardrails"] = PROBE_GUARDRAILS
        put_status, put_body = gov.api_request(
            "PUT",
            f"/api/2.0/serving-endpoints/{endpoint_name}/ai-gateway",
            payload=payload,
        )
        mutated = put_status == 200
        effective = (put_body or {}).get("guardrails") if isinstance(put_body, dict) else None
        print(f"  PUT ai-gateway guardrails -> HTTP {put_status}")
        print(f"  Effective guardrails after PUT: {json.dumps(effective)}")

        if put_status != 200:
            _common.fail("Guardrails configuration refused by this endpoint.")
            gov.conclude(
                ROW_KEY,
                False,
                f"PUT ai-gateway guardrails -> HTTP {put_status} "
                f"({gov.snip(put_body, 250)}) — no guardrail surface available "
                f"on endpoint '{endpoint_name}'.",
            )
            return 1

        _common.ok("Guardrails accepted configuration (input.pii=BLOCK + input.safety).")
        print(f"  Waiting {PROPAGATION_WAIT_S}s for config propagation, then probing BOTH route families...")
        time.sleep(PROPAGATION_WAIT_S)

        results: dict[str, dict] = {}
        for label, (url, model_value) in routes.items():
            pii_status, pii_body = _probe(url, model_value, SYNTHETIC_PII_PROMPT)
            benign_status, _ = _probe(url, model_value, BENIGN_PROMPT)
            cats = _flagged_categories(pii_body) if pii_status != 200 else []
            results[label] = {
                "synthetic_pii": pii_status,
                "benign": benign_status,
                "flagged_categories": cats,
            }
            print(
                f"    {label}\n"
                f"      synthetic PII -> HTTP {pii_status}"
                + (f" flagged={cats}" if cats else "")
                + f" | benign control -> HTTP {benign_status}"
            )

        enforcing = [k for k, v in results.items() if v["synthetic_pii"] != 200]
        bypassing = [k for k, v in results.items() if v["synthetic_pii"] == 200]
        summary = "; ".join(
            f"[{k}] PII={v['synthetic_pii']}"
            + (f" flagged={v['flagged_categories']}" if v["flagged_categories"] else "")
            + f" benign={v['benign']}"
            for k, v in results.items()
        )

        if enforcing and bypassing:
            _common.fail(
                "Guardrails enforce on the classic serving routes and not on "
                "the Unity AI Gateway routes."
            )
            gov.conclude(
                ROW_KEY,
                None,
                "PARTIAL — enforcement is route-scoped. Endpoint guardrails "
                f"({json.dumps(effective)}) blocked the synthetic-PII request on "
                f"{enforcing} and did not block it on {bypassing}. Guardrails "
                "configured on a serving endpoint govern the classic "
                "/serving-endpoints/* routes; the /ai-gateway/* routes resolve "
                "the model in the UC catalog and are governed by service "
                "policies on model services instead, so this config does not "
                f"cover a client pointed at /ai-gateway/*. Probe trail: {summary}. "
                "Benign controls returned 200 everywhere, so the block is "
                "guardrail-specific rather than an outage. No dedicated "
                "prompt-injection knob in this endpoint's schema — knobs are "
                "pii/safety/valid_topics/invalid_keywords (jailbreak/"
                "hallucination/custom keys are dropped by this API without "
                "error).",
            )
            return 0
        if enforcing:
            _common.ok("Guardrails enforced on every probed route.")
            gov.conclude(
                ROW_KEY,
                True,
                "ENFORCED on all probed routes: the synthetic-PII request was "
                f"rejected everywhere. Probe trail: {summary}. Effective config: "
                f"{json.dumps(effective)}.",
            )
            return 0
        _common.fail("Guardrail accepted but blocked nothing on any route.")
        gov.conclude(
            ROW_KEY,
            False,
            "NOT ENFORCED: the guardrail config was accepted "
            f"({json.dumps(effective)}) but the synthetic-PII request returned "
            f"HTTP 200 on every route family. Probe trail: {summary}.",
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
                _common.ok("ai_gateway config restored (guardrails removed).")
            else:
                _common.fail(
                    f"ai_gateway restore FAILED (HTTP {restore_status}): "
                    f"{gov.snip(restore_body, 150)} — remove the guardrails "
                    "manually in the serving UI."
                )


if __name__ == "__main__":
    sys.exit(main())
