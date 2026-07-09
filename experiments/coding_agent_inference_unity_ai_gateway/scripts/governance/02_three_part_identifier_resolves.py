"""Matrix row: three_part_identifier_resolves — UC three-part model ids work.

Claim/source: Unity AI Gateway addresses models by three-part Unity Catalog
identifiers (`system.ai.<model>`), replacing legacy single-string serving
endpoint names.

✅ means: the three-part form itself got HTTP 200 (or a non-auth,
model-level response proving the identifier resolved) through the gateway.
❌ means: the three-part form does not resolve on this workspace (e.g. 404),
regardless of what the legacy name does.

The legacy single-string tail (three-part id with the `system.ai.` prefix
stripped) is also probed — purely as a comparison data point recorded in
notes, never as a pass condition.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gov_common as gov  # noqa: E402
import _common  # noqa: E402

ROW_KEY = "three_part_identifier_resolves"

PROMPT = "Reply with the single word: pong"


def main() -> int:
    _common.section("02 — Three-part UC identifier resolves through the gateway")

    model_id = _common.resolve_model_id()
    legacy_name = gov.endpoint_tail(model_id)
    print(f"  Three-part form: {model_id}")
    print(f"  Legacy single-string comparison probe: {legacy_name}")

    # --- Probe 1: the three-part form (the actual test) -------------------
    status3, body3 = _common.chat_completion(model_id, PROMPT, max_tokens=8)
    print(f"  three-part -> HTTP {status3}")

    # --- Probe 2: legacy single-string tail (comparison only) -------------
    status_legacy, body_legacy = _common.chat_completion(
        legacy_name, PROMPT, max_tokens=8
    )
    print(f"  legacy tail -> HTTP {status_legacy}")

    both = (
        f"three-part '{model_id}' -> HTTP {status3} ({gov.snip(body3, 200)}); "
        f"legacy '{legacy_name}' -> HTTP {status_legacy} "
        f"({gov.snip(body_legacy, 200)})"
    )

    if status3 == 200:
        _common.ok("Three-part identifier resolved and served the request.")
        gov.conclude(ROW_KEY, True, f"Three-part id got HTTP 200. {both}")
        return 0

    body3_text = gov.snip(body3, 400).lower()
    resolved_but_rejected = status3 not in (401, 403, 404) and not any(
        marker in body3_text for marker in ("not found", "does not exist", "no such")
    )
    if resolved_but_rejected:
        _common.ok(
            "Three-part identifier resolved (non-auth, model-level response "
            f"HTTP {status3}) even though the request itself was rejected."
        )
        gov.conclude(
            ROW_KEY,
            True,
            f"Three-part id resolved: non-auth model-level HTTP {status3}. {both}",
        )
        return 0

    _common.fail(f"Three-part identifier did not resolve (HTTP {status3}).")
    gov.conclude(
        ROW_KEY,
        False,
        f"Three-part id failed with HTTP {status3}. {both}",
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
