"""Row 03 — does the gateway forward the caller's Anthropic credential to Anthropic?

Send the two-header shape Claude Code uses through the relay, with a fake Anthropic OAuth token.
If the relay is real, Anthropic itself answers 401 and the gateway reports it as an upstream
error from provider=anthropic. That is the same path a real subscription token takes.
"""
import _row  # noqa: F401
from _common import SUBSCRIPTIONS, classify, record, relayed_messages, section, service, upstream

section("03 relay forwards the caller's credential upstream")
seen = {}
for sub in SUBSCRIPTIONS:
    s, b = relayed_messages("owner", service(sub))
    up = upstream(b)
    seen[sub] = {"status": s, "stopped_at": classify(s, b), "provider": up.get("provider"),
                 "upstream_status": up.get("upstream_status"),
                 "upstream_says": "OAuth access token is invalid." if "OAuth access token is invalid" in
                 str(up.get("upstream_body", "")) else str(up.get("upstream_body", ""))[:80]}
good = all(v["stopped_at"] == "reached_anthropic" and v["upstream_status"] == "401" for v in seen.values())
record("03_relay_forwards_caller_credential", "✅" if good else "❌",
       "fake subscription token reached Anthropic on every service: gateway returns upstream 401 "
       "'OAuth access token is invalid.' from provider=anthropic. Nothing stored, nothing swapped in"
       if good else f"unexpected: {seen}", seen)
