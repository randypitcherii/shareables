import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from importlib import import_module

generate = import_module("00_generate_events")


def test_event_shape():
    raw = generate.build_event(seq=42, payload_bytes=0)
    event = json.loads(raw)
    assert event["seq"] == 42
    assert set(event) == {"event_id", "seq", "event_ts", "device_id", "region", "value", "payload"}
    assert isinstance(event["payload"], dict)
    assert len(event["payload"]["readings"]) == 5


def test_event_padding_hits_target_size():
    target = 512
    raw = generate.build_event(seq=1, payload_bytes=target)
    assert abs(len(raw) - target) <= 32  # json overhead tolerance
    event = json.loads(raw)
    assert "pad" in event["payload"]


def test_small_target_means_no_padding():
    raw = generate.build_event(seq=1, payload_bytes=1)
    assert "pad" not in json.loads(raw)["payload"]
