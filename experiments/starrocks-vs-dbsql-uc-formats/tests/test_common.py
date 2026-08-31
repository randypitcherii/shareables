import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import pytest
from _common import (
    AGG_QUERIES,
    CHECKSUM_SQL,
    dbsql_cost_usd,
    gen_select_dbsql,
    gen_select_starrocks,
    starrocks_cost_usd,
    timing_stats,
)


def test_timing_stats_shape():
    stats = timing_stats([0.5, 0.1, 0.3])
    assert stats["runs"] == 3
    assert stats["first_run_ms"] == 500
    assert stats["min_ms"] == 100
    assert stats["max_ms"] == 500
    assert stats["p50_ms"] == 300
    assert stats["all_ms"] == [500, 100, 300]


def test_timing_stats_empty():
    assert timing_stats([]) == {}


def test_dbsql_cost_scales_with_dbu_and_time():
    # Medium (24 DBU/hr) at $0.70/DBU for one hour = $16.80
    assert dbsql_cost_usd(24, 0.70, 3600) == pytest.approx(16.8)
    assert dbsql_cost_usd(24, 0.70, 36) == pytest.approx(0.168)


def test_starrocks_cost_uses_ec2_rate():
    class Cfg:
        ec2_usd_per_hour = 0.384

    assert starrocks_cost_usd(Cfg, 3600) == pytest.approx(0.384)
    assert starrocks_cost_usd(Cfg, 60) == pytest.approx(0.0064)


def test_generators_are_deterministic_and_dialect_correct():
    db = gen_select_dbsql(100000)
    sr = gen_select_starrocks(100000)
    # Same logical dataset: shared integer-derived expressions on both sides.
    for frag in ("% 5000", "% 86400", "% 100000", "repeat('x', 200)", "lpad("):
        assert frag in db and frag in sr
    assert "FROM range(1, 100001)" in db
    assert "generate_series(1, 100001)" in sr
    assert "TIMESTAMP_NTZ" in db and "DATETIME" in sr
    assert "STRING" in db and "VARCHAR" in sr


def test_checksum_and_aggs_format_with_table():
    sql = CHECKSUM_SQL.format(table="cat.sch.t")
    assert "FROM cat.sch.t" in sql and "COUNT(DISTINCT device_id)" in sql
    assert set(AGG_QUERIES) == {"agg_group_by", "agg_filter", "agg_distinct"}
    for q in AGG_QUERIES.values():
        assert "{table}" in q and "{table}" not in q.format(table="t")


def test_record_result_fails_closed_without_identity(tmp_path, monkeypatch):
    import _common

    monkeypatch.setattr(_common, "RESULTS_PATH", tmp_path / "matrix_results.json")
    with pytest.raises(SystemExit):
        _common.record_result("row", {"ops": {}})
    _common.record_result("row", {"ops": {}, "identity": {"user": "someone"}})
    assert (tmp_path / "matrix_results.json").exists()


def test_record_result_rerun_supersedes_without_erasing(tmp_path, monkeypatch):
    """A rerun under a key suffix must leave the first attempt's row intact."""
    import json

    import _common

    results = tmp_path / "matrix_results.json"
    monkeypatch.setattr(_common, "RESULTS_PATH", results)
    monkeypatch.delenv("RESULT_KEY_SUFFIX", raising=False)
    monkeypatch.delenv("RESULT_RUN_NOTE", raising=False)

    _common.record_result("cell", {"identity": {"user": "u"}, "status": "environment_blocked"})

    monkeypatch.setenv("RESULT_KEY_SUFFIX", "__rerun")
    monkeypatch.setenv("RESULT_RUN_NOTE", "workspace with no IP access list")
    _common.record_result("cell", {"identity": {"user": "u"}, "status": "ok"})

    data = json.loads(results.read_text())
    assert data["cell"]["status"] == "environment_blocked"
    assert data["cell__rerun"]["status"] == "ok"
    assert data["cell__rerun"]["run_note"] == "workspace with no IP access list"
