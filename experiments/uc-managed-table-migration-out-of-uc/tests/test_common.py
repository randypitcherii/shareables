import json

import _common
import pytest


def test_invalid_status_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "RESULTS", tmp_path / "results.json")
    with pytest.raises(ValueError):
        _common.write_result("1", question="q", status="maybe", finding="f", evidence={})


def test_results_are_scrubbed(tmp_path, monkeypatch):
    target = tmp_path / "results.json"
    monkeypatch.setattr(_common, "RESULTS", target)
    monkeypatch.setattr(_common, "EXTERNAL_ROOT", "s3://real-private-bucket/path")
    monkeypatch.setattr(_common, "MANAGED_CATALOG", "acme_secret_catalog")
    _common.write_result(
        "1",
        question="q",
        status="pass",
        finding="owner@example.com wrote s3://real-private-bucket/path/table in acme_secret_catalog",
        evidence={
            "url": "https://private.cloud.databricks.com/path",
            "id": "RequestId=abc-123 x",
            "path": "tables/94604deb-b36a-4db5-a0b1-b82feb105a93",
        },
    )
    blob = target.read_text()
    for secret in (
        "real-private-bucket",
        "owner@example.com",
        "private.cloud",
        "acme_secret",
        "abc-123",
    ):
        assert secret not in blob
    assert json.loads(blob)["rows"]["1"]["status"] == "pass"
