"""Row 04 — does Unity Catalog EXECUTE gate each subscription's service before the relay?

The test caller holds EXECUTE on sub_a only. Expect sub_a to reach Anthropic (fake token → 401
upstream) and sub_b / sub_c to stop at Unity Catalog (403) without ever reaching Anthropic.
That is how a 400-person org keeps group A on subscription A's service.
"""
import _row  # noqa: F401
from _common import classify, record, relayed_messages, section, service

section("04 EXECUTE gates each service before the relay")
seen = {}
for sub in ("sub_a", "sub_b", "sub_c"):
    s, b = relayed_messages("test_caller", service(sub))
    seen[sub] = {"status": s, "stopped_at": classify(s, b), "message": str(b.get("message", ""))[:160]}
good = (seen["sub_a"]["stopped_at"] in {"reached_anthropic", "rate_limited"}
        and seen["sub_b"]["stopped_at"] == "denied_by_uc" and seen["sub_c"]["stopped_at"] == "denied_by_uc")
record("04_execute_gates_each_service", "✅" if good else "❌",
       f"test caller (EXECUTE on sub_a only): sub_a {seen['sub_a']['stopped_at']} "
       f"(HTTP {seen['sub_a']['status']}), sub_b HTTP {seen['sub_b']['status']} {seen['sub_b']['stopped_at']}, "
       f"sub_c HTTP {seen['sub_c']['status']} {seen['sub_c']['stopped_at']}", seen)
