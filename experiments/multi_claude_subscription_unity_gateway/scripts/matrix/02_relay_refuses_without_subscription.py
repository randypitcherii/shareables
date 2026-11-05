"""Row 02 — does a relayed service refuse a call that carries no subscription token?

A client that sends only its Databricks token in Authorization (the normal gateway shape) gives
the relay nothing to forward. Expect the gateway itself to refuse, on every service.
"""
import _row  # noqa: F401
from _common import SUBSCRIPTIONS, classify, record, relayed_messages, section, service

section("02 relay refuses a call with no subscription token")
seen = {}
for sub in SUBSCRIPTIONS:
    s, b = relayed_messages("owner", service(sub), swap_header=False)
    seen[sub] = {"status": s, "stopped_at": classify(s, b), "message": str(b.get("message", ""))[:160]}
good = all(v["stopped_at"] == "relay_refused" for v in seen.values())
record("02_relay_refuses_without_subscription", "✅" if good else "❌",
       "every service answers 400 'uses relayed authentication and can only be invoked by a relayed "
       "(credential-swapped) request' when no Anthropic credential rides along" if good
       else f"unexpected: {seen}", seen)
