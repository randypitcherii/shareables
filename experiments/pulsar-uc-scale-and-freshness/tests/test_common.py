"""Unit tests for shared helpers: latency stats, freshness model, results writing."""

import importlib.util
import json
from pathlib import Path

import pytest

EXPERIMENT_ROOT = Path(__file__).resolve().parent.parent


def _load_common():
    path = EXPERIMENT_ROOT / "scripts" / "_common.py"
    spec = importlib.util.spec_from_file_location("common", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


COMMON = _load_common()


def test_latency_stats_percentiles():
    stats = COMMON.latency_stats_ms(list(range(1, 101)))
    assert stats["count"] == 100
    assert stats["p50_ms"] == 51
    assert stats["p95_ms"] == 96
    assert stats["max_ms"] == 100


def test_latency_stats_empty():
    assert COMMON.latency_stats_ms([]) == {}


def test_modeled_freshness_shape():
    # 60s trigger, drain 50k rows/s, producing 5k rows/s:
    # wait p95 = 57s, processing = 300k/50k = 6s -> 63s
    assert COMMON.modeled_freshness_p95_sec(60, 50_000, 5_000) == pytest.approx(63.0)
    # longer triggers scale linearly in both terms
    assert COMMON.modeled_freshness_p95_sec(3600, 50_000, 5_000) == pytest.approx(0.95 * 3600 + 360)


def test_modeled_freshness_rejects_bad_inputs():
    with pytest.raises(ValueError):
        COMMON.modeled_freshness_p95_sec(0, 1000, 1000)
    with pytest.raises(ValueError):
        COMMON.modeled_freshness_p95_sec(60, 0, 1000)


def test_record_result_merges(tmp_path, monkeypatch):
    target = tmp_path / "matrix_results.json"
    monkeypatch.setattr(COMMON, "RESULTS_PATH", target)
    COMMON.record_result("alpha", {"x": 1})
    COMMON.record_result("beta", {"y": 2})
    data = json.loads(target.read_text())
    assert data["alpha"]["x"] == 1
    assert data["beta"]["y"] == 2
    assert "recorded_at" in data["alpha"]
