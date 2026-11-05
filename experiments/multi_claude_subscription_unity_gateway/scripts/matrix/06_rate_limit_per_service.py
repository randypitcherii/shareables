"""Row 06 — does the gateway throttle a subscription's service before the relay?

dev deploys sub_a with a 2-requests-per-minute limit on the test caller. Send 4 quickly: expect
the first ones to reach Anthropic and the rest to stop at the gateway with 429. In a rollout the
same knob (RATE_LIMIT_KEY_SERVICE) caps a whole subscription's traffic below Anthropic's limit.
"""
import _row  # noqa: F401
from _common import classify, record, relayed_messages, section, service

section("06 per-service rate limit before the relay")
trail = []
for _ in range(4):
    s, b = relayed_messages("test_caller", service("sub_a"))
    trail.append(classify(s, b))
good = "rate_limited" in trail and trail.index("rate_limited") >= 1 and trail[0] == "reached_anthropic"
some = "rate_limited" in trail
record("06_rate_limit_per_service", "✅" if good else ("◑" if some else "❌"),
       f"4 calls in a row on sub_a: {' → '.join(trail)}. A throttled call never reaches Anthropic, "
       "so it spends none of that subscription's limit", {"trail": trail})
