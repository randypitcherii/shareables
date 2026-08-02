"""Produce synthetic JSON events to the evaluation topic with the native Pulsar client.

One event stream feeds every ingestion path: the broker runs KoP with
entryFormat=pulsar, so events produced here are consumable over both the Pulsar
binary protocol and the Kafka protocol.

Event shape (generic): identifiers, an event-time timestamp for freshness math,
a couple of typed dimensions, and a nested JSON payload padded to
EVENT_PAYLOAD_BYTES to exercise semi-structured handling at a controlled size.
"""

import json
import random
import string
import time
import uuid

import pulsar
from _common import load_config, record_result


def build_event(seq: int, payload_bytes: int) -> bytes:
    event = {
        "event_id": str(uuid.uuid4()),
        "seq": seq,
        "event_ts": int(time.time() * 1000),
        "device_id": f"device-{seq % 500:04d}",
        "region": random.choice(["us-east", "us-west", "eu-central", "ap-south"]),
        "value": round(random.uniform(0, 1000), 3),
        "payload": {
            "firmware": f"{random.randint(1, 4)}.{random.randint(0, 9)}.{random.randint(0, 20)}",
            "readings": [round(random.gauss(50, 10), 2) for _ in range(5)],
            "flags": {"calibrated": seq % 7 != 0, "battery_pct": random.randint(1, 100)},
        },
    }
    raw = json.dumps(event, separators=(",", ":")).encode()
    if len(raw) < payload_bytes:
        pad = "".join(random.choices(string.ascii_lowercase, k=payload_bytes - len(raw) - 14))
        event["payload"]["pad"] = pad
        raw = json.dumps(event, separators=(",", ":")).encode()
    return raw


def main() -> None:
    cfg = load_config()
    client = pulsar.Client(cfg.pulsar_service_url)
    producer = client.create_producer(
        cfg.pulsar_topic,
        block_if_queue_full=True,
        batching_enabled=True,
        batching_max_publish_delay_ms=10,
    )

    started = time.time()
    interval = 1.0 / cfg.event_rate_per_sec if cfg.event_rate_per_sec > 0 else 0
    next_send = time.monotonic()
    for seq in range(cfg.event_count):
        producer.send_async(build_event(seq, cfg.event_payload_bytes), callback=lambda *_: None)
        if interval:
            next_send += interval
            sleep_for = next_send - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
        if seq and seq % 20000 == 0:
            print(f"produced {seq} events...")
    producer.flush()
    elapsed = time.time() - started
    client.close()

    summary = {
        "events_produced": cfg.event_count,
        "target_rate_per_sec": cfg.event_rate_per_sec,
        "achieved_rate_per_sec": round(cfg.event_count / elapsed),
        "payload_bytes": cfg.event_payload_bytes,
        "elapsed_sec": round(elapsed, 1),
        "topic": cfg.pulsar_topic,
    }
    print(json.dumps(summary, indent=2))
    record_result("generator", summary)


if __name__ == "__main__":
    main()
