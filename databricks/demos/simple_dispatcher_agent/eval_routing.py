"""
Golden-set check for the router: does JEV send each request where we expect?

Run after editing a route criterion or ROUTER_INSTRUCTIONS. It calls JEV only
(no chat models), so a full run costs a few hundred input tokens per prompt.

    uv run python eval_routing.py
"""

from __future__ import annotations

import sys

import agent

GOLDEN = {
    "luna": [
        "Reformat as CSV: apples 3, pears 5",
        "Summarize this incident log into 5 bullets: " + "ERROR timeout at node-7 ... " * 400,
        "What's the capital of Peru?",
        "Translate 'good morning team' into Spanish",
    ],
    "opus": [
        "Design a fault-tolerant exactly-once Kafka to Delta pipeline and explain the trade-offs",
        "Find the race condition in this Go code that shares a map across goroutines",
        "Prove there are infinitely many primes",
        "Should we shard by tenant or by region? Walk through failure modes, cost, and migration risk.",
    ],
    "argon": [
        "Write a tagline for a hiking boot brand",
        "Sketch a dashboard layout for a fleet tracking app",
        "How should I orient a 3D-printed bracket so it is strongest under a downward load?",
        "Write a keynote opening paragraph that gives the audience chills",
    ],
}


def main() -> int:
    cfg, client = agent.load_config(), agent.GatewayClient()
    misses = 0
    for want, prompts in GOLDEN.items():
        for prompt in prompts:
            d = agent.route_request(client, [{"role": "user", "content": prompt}], cfg)
            ok = d.route == want
            misses += not ok
            conf = f"{d.confidence:.2f}" if d.confidence is not None else " -- "
            print(f"{'✅' if ok else '❌'} want={want:<5} got={d.route:<5} conf={conf}  {prompt[:60]}")
    total = sum(map(len, GOLDEN.values()))
    print(f"\n{total - misses}/{total} routed as expected")
    return 1 if misses else 0


if __name__ == "__main__":
    sys.exit(main())
