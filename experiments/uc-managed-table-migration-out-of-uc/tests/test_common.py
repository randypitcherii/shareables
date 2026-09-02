import json

import _common
import pytest


def test_invalid_status_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "RESULTS", tmp_path / "results.json")
    with pytest.raises(ValueError):
        _common.write_result("1", question="q", status="maybe", finding="f", evidence={})


def test_external_cleanup_refuses_path_outside_root(monkeypatch):
    monkeypatch.setattr(_common, "EXTERNAL_ROOT", "s3://experiment/root")
    with pytest.raises(ValueError):
        _common.clear_external_path("s3://another-bucket/path")


def test_results_are_scrubbed(tmp_path, monkeypatch):
    target = tmp_path / "results.json"
    monkeypatch.setattr(_common, "RESULTS", target)
    monkeypatch.setattr(_common, "EXTERNAL_ROOT", "s3://real-private-bucket/path")
    _common.write_result(
        "1",
        question="q",
        status="pass",
        finding="owner@example.com wrote s3://real-private-bucket/path/table",
        evidence={"url": "https://private.cloud.databricks.com/path"},
    )
    blob = target.read_text()
    assert "real-private-bucket" not in blob
    assert "owner@example.com" not in blob
    assert "private.cloud.databricks.com" not in blob
    assert json.loads(blob)["rows"]["1"]["status"] == "pass"
