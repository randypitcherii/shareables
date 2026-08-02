import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import _common


def test_latency_stats_empty():
    assert _common.latency_stats_ms([]) == {}


def test_latency_stats_percentiles_ordered():
    stats = _common.latency_stats_ms([float(i) for i in range(1000)])
    assert stats["count"] == 1000
    assert stats["p50_ms"] <= stats["p95_ms"] <= stats["p99_ms"] <= stats["max_ms"]
    assert stats["max_ms"] == 999


def test_load_config_requires_core_keys(monkeypatch):
    for key in ("PULSAR_SERVICE_URL", "UC_CATALOG", "APP_ENV"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("APP_ENV", "nonexistent-env-file")
    try:
        _common.load_config()
        raised = False
    except SystemExit:
        raised = True
    assert raised


def test_load_config_reads_env(monkeypatch):
    monkeypatch.setenv("APP_ENV", "nonexistent-env-file")
    monkeypatch.setenv("PULSAR_SERVICE_URL", "pulsar://203.0.113.5:6650")
    monkeypatch.setenv("UC_CATALOG", "some_catalog")
    cfg = _common.load_config()
    assert cfg.pulsar_service_url == "pulsar://203.0.113.5:6650"
    assert cfg.uc_schema == "pulsar_uc_ingest_eval"
    assert cfg.event_count > 0
