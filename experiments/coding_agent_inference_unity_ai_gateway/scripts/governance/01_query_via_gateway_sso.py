"""Matrix row: query_via_gateway_sso — baseline SSO inference via the gateway.

Claim/source: Databricks docs say workspace users can query foundation models
through the AI Gateway / serving endpoints using their own OAuth (SSO)
identity — no PAT required.

✅ means: one real chat completion against the resolved RPW_MODEL (three-part
Unity Catalog identifier) returned HTTP 200 using ONLY a short-lived SSO
OAuth token.
❌ means: the call failed (auth or otherwise) — the response body is captured
verbatim in notes.

This row is the gate for the whole matrix: if the baseline SSO query fails,
every other row's ✅/❌ is meaningless noise.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402  (adds scripts/ to sys.path)
import _common  # noqa: E402

ROW_KEY = "query_via_gateway_sso"


def main() -> int:
    _common.section("01 — Baseline: query via gateway with SSO OAuth only")
    print("  This row must pass before the other nine rows mean anything —")
    print("  it proves the SSO token + three-part identifier path end to end.")

    model_id = _common.resolve_model_id()
    print(f"  Model (three-part UC id): {model_id}")
    print(f"  Profile: {_common.get_profile()}  Host: {_common.get_host()}")

    status, body = _common.chat_completion(
        model_id, "Reply with the single word: pong", max_tokens=16
    )
    print(f"  HTTP status: {status}")

    if status == 200:
        _common.ok("SSO-only inference through the gateway succeeded (HTTP 200).")
        content = ""
        if isinstance(body, dict):
            try:
                content = body["choices"][0]["message"]["content"]
            except (KeyError, IndexError, TypeError):
                content = gov.snip(body, 120)
        notes = (
            f"HTTP 200 with SSO OAuth token only (zero PATs); model={model_id}; "
            f"reply={gov.snip(content, 120)}. Baseline gate for all other rows."
        )
        gov.conclude(ROW_KEY, True, notes)
        return 0

    _common.fail(f"Inference failed with HTTP {status}.")
    notes = (
        f"HTTP {status} for model={model_id} using SSO OAuth token only. "
        f"Body: {gov.snip(body)}. Baseline gate FAILED — treat every other "
        "matrix row as unproven until this passes."
    )
    gov.conclude(ROW_KEY, False, notes)
    return 1


if __name__ == "__main__":
    sys.exit(main())
