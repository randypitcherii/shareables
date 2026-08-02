"""Produce realistic ~1KB nested JSON events to the evaluation topic.

Standalone by design: config comes ONLY from environment variables so the same
file runs unchanged on a laptop (via `make produce`) or ON the broker VM inside
the pre-built producer container (via `make produce-remote`), where localhost
bandwidth keeps the producer out of the measurement.

Event population (weights in EVENT_TYPE_WEIGHTS):
- ~35% business events (send/open/click/purchase/bounce/unsubscribe) — the rows
  a production pipeline should land.
- ~65% noise (debug_log/heartbeat/internal_metric) — the firehose surplus that
  pre-table filtering must drop.

Payloads are heterogeneous per type: arrays of objects, optional keys, mixed
types (string/int/float/bool/null), skewed multi-tenant project ids, and
natural size jitter averaging ~1KB. No random padding — realism over neatness.

Env config:
  PULSAR_SERVICE_URL (required)   pulsar://host:6650
  PULSAR_TOPIC                    default persistent://public/default/uc-scale-eval
  EVENT_COUNT                     default 100000; <=0 with DURATION_SEC>0 means run for duration
  EVENT_RATE_PER_SEC              default 0 (max sustainable)
  DURATION_SEC                    default 0 (bounded by EVENT_COUNT instead)
  GENERATOR_SEED                  default unset (nondeterministic)

Prints a final line "SUMMARY_JSON: {...}" that callers parse for results.
"""

import json
import os
import random
import time
import uuid

KEEP_EVENT_TYPES = ("send", "open", "click", "purchase", "bounce", "unsubscribe")
NOISE_EVENT_TYPES = ("debug_log", "heartbeat", "internal_metric")

EVENT_TYPE_WEIGHTS = {
    "send": 0.18,
    "open": 0.09,
    "click": 0.05,
    "purchase": 0.02,
    "bounce": 0.008,
    "unsubscribe": 0.002,
    "debug_log": 0.40,
    "heartbeat": 0.15,
    "internal_metric": 0.10,
}

_TYPES = list(EVENT_TYPE_WEIGHTS)
_WEIGHTS = list(EVENT_TYPE_WEIGHTS.values())

N_PROJECTS = 200
LOCALES = ["en_US", "en_GB", "de_DE", "fr_FR", "ja_JP", "pt_BR", "es_MX", "hi_IN"]
COUNTRIES = ["US", "GB", "DE", "FR", "JP", "BR", "MX", "IN", "CA", "AU"]
USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36 Edg/126.0.2592.87",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.6478.122 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
]
SMTP_REASONS = [
    "mailbox_full",
    "invalid_recipient",
    "policy_rejection",
    "greylisted",
    "dns_failure",
]
LOGGERS = ["ingest.router", "delivery.smtp", "profile.sync", "segment.eval", "webhook.dispatch"]
SERVICES = ["edge-gw", "renderer", "scheduler", "tracker", "segmenter"]


def _project_id(rng: random.Random) -> str:
    """Zipf-ish tenant skew: low ids are hot, long tail is cold."""
    r = rng.random()
    idx = int(N_PROJECTS * (r**3))  # cubic skew: ~top-10 tenants take ~37% of traffic
    return f"proj-{min(idx, N_PROJECTS - 1):04d}"


def _maybe(rng: random.Random, p: float, value):
    return value if rng.random() < p else None


def _prune(obj):
    """Drop None values so optional keys are genuinely absent, not null — except
    a few deliberately kept nulls for mixed-type realism (handled by callers)."""
    if isinstance(obj, dict):
        return {k: _prune(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_prune(v) for v in obj]
    return obj


def _base(rng: random.Random, seq: int, event_type: str) -> dict:
    user_hash = f"{rng.getrandbits(128):032x}"
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "seq": seq,
        "event_ts": int(time.time() * 1000),
        "event_type": event_type,
        "project_id": _project_id(rng),
        "schema_version": rng.choice([2, 3, 3, 3, "3.1"]),  # mixed int/str on purpose
        "user": _prune(
            {
                "user_hash": user_hash,
                "locale": rng.choice(LOCALES),
                "email_domain": _maybe(
                    rng, 0.8, rng.choice(["gmail.com", "yahoo.com", "corp.example"])
                ),
                "consent": _maybe(rng, 0.6, {"marketing": rng.random() < 0.9, "analytics": True}),
            }
        ),
        "context": _prune(
            {
                "user_agent": rng.choice(USER_AGENTS),
                "ip_hash": f"{rng.getrandbits(64):016x}",
                "geo": {
                    "country": rng.choice(COUNTRIES),
                    "region": f"region-{rng.randint(1, 40)}",
                    "city": _maybe(rng, 0.7, f"city-{rng.randint(1, 400)}"),
                },
                "session": _maybe(
                    rng,
                    0.5,
                    {"id": f"{rng.getrandbits(64):016x}", "depth": rng.randint(1, 30)},
                ),
                "trace": {
                    "trace_id": f"{rng.getrandbits(128):032x}",
                    "span_id": f"{rng.getrandbits(64):016x}",
                    "sampled": rng.random() < 0.2,
                },
            }
        ),
        "sdk": {
            "name": rng.choice(["ingest-js", "ingest-swift", "ingest-kotlin", "server-api"]),
            "version": f"{rng.randint(2, 9)}.{rng.randint(0, 30)}.{rng.randint(0, 9)}",
            "platform": rng.choice(["web", "ios", "android", "backend"]),
        },
        "labels": _maybe(
            rng,
            0.6,
            rng.sample(
                ["beta-cohort", "eu-resident", "vip", "re-engaged", "suppressed", "test-seed"],
                rng.randint(1, 3),
            ),
        ),
    }


def _campaign(rng: random.Random) -> dict:
    return {
        "id": rng.randint(10_000, 99_999),
        "template_id": rng.randint(100, 999),
        "variation": rng.choice(["control", "a", "b", None]),
    }


def _properties(rng: random.Random, event_type: str) -> dict:
    if event_type == "send":
        return {
            "campaign": _campaign(rng),
            "message_id": f"{rng.getrandbits(96):024x}",
            "channel": rng.choice(["email", "push", "sms", "in_app"]),
            "headers": [
                {"k": "x-priority", "v": rng.choice(["high", "normal"])},
                {"k": "x-retry", "v": rng.randint(0, 3)},
            ],
        }
    if event_type == "open":
        return {
            "campaign": _campaign(rng),
            "message_id": f"{rng.getrandbits(96):024x}",
            "device": {
                "os": rng.choice(["ios", "android", "macos", "windows"]),
                "os_version": f"{rng.randint(10, 18)}.{rng.randint(0, 6)}",
                "app_build": _maybe(rng, 0.5, rng.randint(1000, 9999)),
            },
            "is_bot": rng.random() < 0.08,
        }
    if event_type == "click":
        return {
            "campaign": _campaign(rng),
            "message_id": f"{rng.getrandbits(96):024x}",
            "url": f"https://links.example/{rng.getrandbits(48):012x}?c={rng.randint(1, 9)}",
            "link_index": rng.randint(0, 12),
            "params": [
                {"k": f"utm_{k}", "v": f"v{rng.randint(1, 50)}"}
                for k in rng.sample(["source", "medium", "campaign", "term"], rng.randint(1, 3))
            ],
        }
    if event_type == "purchase":
        items = [
            _prune(
                {
                    "sku": f"sku-{rng.randint(1, 5000):05d}",
                    "name": f"item {rng.randint(1, 5000)}",
                    "qty": rng.randint(1, 5),
                    "price": round(rng.uniform(3, 400), 2),
                    "attrs": _maybe(
                        rng,
                        0.7,
                        {
                            "color": rng.choice(["red", "blue", "black", None]),
                            "size": rng.choice(["s", "m", "l", rng.randint(30, 46)]),
                            "tags": rng.sample(
                                ["sale", "new", "bundle", "gift"], rng.randint(0, 3)
                            ),
                        },
                    ),
                }
            )
            for _ in range(rng.randint(1, 5))
        ]
        return {
            "order_id": f"ord-{rng.getrandbits(64):016x}",
            "currency": rng.choice(["USD", "EUR", "JPY"]),
            "total": round(sum(i["qty"] * i["price"] for i in items), 2),
            "items": items,
            "campaign": _maybe(rng, 0.6, _campaign(rng)),
        }
    if event_type == "bounce":
        return {
            "message_id": f"{rng.getrandbits(96):024x}",
            "reason": rng.choice(SMTP_REASONS),
            "smtp_code": rng.choice([550, 552, "4.2.2", 421]),  # mixed int/str on purpose
            "diagnostic": f"smtp; {rng.randint(400, 599)} {rng.choice(SMTP_REASONS)} for recipient",
            "permanent": rng.random() < 0.6,
        }
    if event_type == "unsubscribe":
        return {
            "list_ids": [rng.randint(1, 900) for _ in range(rng.randint(1, 6))],
            "source": rng.choice(["link", "preference_center", "complaint", "api"]),
        }
    if event_type == "debug_log":
        return {
            "level": rng.choice(["DEBUG", "TRACE", "INFO"]),
            "logger": rng.choice(LOGGERS),
            "msg": f"processed batch {rng.randint(1, 10**6)} in {rng.randint(1, 900)}ms "
            f"(queue={rng.randint(0, 5000)}, retries={rng.randint(0, 3)})",
            "stack": _maybe(
                rng,
                0.3,
                [
                    f"{rng.choice(LOGGERS)}.fn_{rng.randint(1, 99)}:{rng.randint(10, 999)}"
                    for _ in range(rng.randint(2, 6))
                ],
            ),
            "ctx": {
                "host": f"ip-10-{rng.randint(0, 255)}-{rng.randint(0, 255)}-{rng.randint(0, 255)}",
                "pod": f"{rng.choice(SERVICES)}-{rng.getrandbits(20):05x}",
                "thread": f"worker-{rng.randint(0, 63)}",
                "kv": {
                    f"attempt_{j}": rng.choice([True, False, rng.randint(0, 9)])
                    for j in range(rng.randint(1, 4))
                },
            },
        }
    if event_type == "heartbeat":
        return {
            "service": rng.choice(SERVICES),
            "uptime_s": rng.randint(60, 10**7),
            "metrics": {
                "cpu_pct": round(rng.uniform(1, 95), 1),
                "mem_mb": rng.randint(200, 16000),
                "inflight": rng.randint(0, 2000),
                "lag": _maybe(rng, 0.5, rng.randint(0, 100000)),
            },
            "checks": [
                {
                    "name": rng.choice(["db", "cache", "bus", "dns", "disk"]),
                    "ok": rng.random() < 0.97,
                    "latency_ms": round(rng.uniform(0.2, 40), 2),
                }
                for _ in range(rng.randint(3, 6))
            ],
        }
    # internal_metric
    return {
        "metric_name": f"{rng.choice(SERVICES)}.{rng.choice(['qps', 'p99_ms', 'errors', 'bytes'])}",
        "value": round(rng.uniform(0, 100000), 3),
        "dims": {f"d{j}": f"v{rng.randint(1, 20)}" for j in range(rng.randint(1, 5))},
        "histogram": [round(rng.uniform(0, 1000), 2) for _ in range(rng.randint(8, 12))],
    }


def build_event(rng: random.Random, seq: int) -> bytes:
    event_type = rng.choices(_TYPES, weights=_WEIGHTS, k=1)[0]
    event = _base(rng, seq, event_type)
    event["properties"] = _properties(rng, event_type)
    return json.dumps(event, separators=(",", ":")).encode()


def main() -> None:  # pragma: no cover - exercised live
    import pulsar

    service_url = os.environ["PULSAR_SERVICE_URL"]
    topic = os.environ.get("PULSAR_TOPIC", "persistent://public/default/uc-scale-eval")
    count = int(os.environ.get("EVENT_COUNT", "100000"))
    rate = int(os.environ.get("EVENT_RATE_PER_SEC", "0"))
    duration = int(os.environ.get("DURATION_SEC", "0"))
    seed = os.environ.get("GENERATOR_SEED")
    rng = random.Random(int(seed)) if seed else random.Random()

    client = pulsar.Client(service_url, io_threads=4)
    producer = client.create_producer(
        topic,
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=10,
        max_pending_messages=50000,
    )

    started = time.time()
    deadline = started + duration if duration > 0 else None
    interval = 1.0 / rate if rate > 0 else 0
    next_send = time.monotonic()
    sent = 0
    bytes_sent = 0
    seq = 0
    while True:
        if deadline is not None and time.time() >= deadline:
            break
        if deadline is None and seq >= count:
            break
        raw = build_event(rng, seq)
        bytes_sent += len(raw)
        producer.send_async(raw, callback=lambda *_: None)
        seq += 1
        sent += 1
        if interval:
            next_send += interval
            sleep_for = next_send - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
        if sent % 100000 == 0:
            elapsed = time.time() - started
            print(f"produced {sent} events ({round(sent / elapsed)}/s)...", flush=True)
    producer.flush()
    elapsed = time.time() - started
    client.close()

    summary = {
        "events_produced": sent,
        "target_rate_per_sec": rate,
        "achieved_rate_per_sec": round(sent / elapsed) if elapsed else 0,
        "avg_event_bytes": round(bytes_sent / sent) if sent else 0,
        "elapsed_sec": round(elapsed, 1),
        "duration_mode": duration > 0,
        "topic": topic,
    }
    print("SUMMARY_JSON: " + json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
