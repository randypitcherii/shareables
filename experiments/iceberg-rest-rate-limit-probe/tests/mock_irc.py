"""Local stand-in for the Iceberg REST catalog with a per-route token bucket.

Lets you exercise the probe, live graph and analysis end-to-end with no workspace:
    uv run python tests/mock_irc.py --port 9999 &
    DATABRICKS_PROFILE_A=mock=http://127.0.0.1:9999 UC_CATALOG=c make probe ENDPOINT=config

Limits (refill/s, bucket) are illustrative only, NOT Databricks' numbers.
"""

import argparse
import asyncio
import random
import time

from aiohttp import web

LIMITS = {"config": (40, 120), "list": (50, 50), "load": (450, 1350), "legacy": (5, 5)}


class Bucket:
    def __init__(self, rate: float, size: float) -> None:
        self.rate, self.size, self.tokens, self.t = rate, size, size, time.monotonic()

    def take(self) -> bool:
        now = time.monotonic()
        self.tokens = min(self.size, self.tokens + (now - self.t) * self.rate)
        self.t = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


def route(path: str) -> str:
    if "/unity-catalog/iceberg/" in path:
        return "legacy"
    if path.endswith("/config"):
        return "config"
    if path.endswith("/tables"):
        return "list"
    return "load"


def make_app() -> web.Application:
    buckets = {k: Bucket(*v) for k, v in LIMITS.items()}

    async def handle(req: web.Request) -> web.Response:
        r = route(req.path)
        await asyncio.sleep(random.uniform(0.01, 0.04))
        if not buckets[r].take():
            return web.json_response(
                {"error_code": "REQUEST_LIMIT_EXCEEDED", "message": f"mock {r} limit"},
                status=429, headers={"Retry-After": "1"})
        if r == "config":
            return web.json_response({"defaults": {}, "overrides": {"prefix": "catalogs/c"}})
        return web.json_response({"identifiers": []} if r == "list" else {"metadata": {}})

    app = web.Application()
    app.router.add_get("/{tail:.*}", handle)
    return app


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=9999)
    web.run_app(make_app(), host="127.0.0.1", port=p.parse_args().port, print=None)
