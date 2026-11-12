import json

import _common
import pytest


def test_scrub_redacts_coordinates():
    raw = (
        "user randy@example.org at https://dbc-123.cloud.databricks.com/api read "
        "s3://real-bucket-123456789012/some/prefix/x.parquet in bucket 'real-bucket-123456789012' "
        "arn:aws:sts::123456789012:assumed-role/r/m=abc,s=ABCDEFGHIJKLMNOPQRS "
        "table 49d16214-69ca-42fb-9e3b-8ac6d7cbcfad token eyJabc.def.ghi"
    )
    out = _common.scrub(raw)
    for leaked in ("randy@", "dbc-123", "real-bucket", "123456789012", "49d16214", "eyJabc", "ABCDEFGHIJ"):
        assert leaked not in out, leaked


def test_create_table_sql_shapes():
    assert "USING ICEBERG" in _common.create_table_sql("t", "iceberg")
    assert "'format-version' = '3'" in _common.create_table_sql("t", "iceberg")
    v2 = _common.create_table_sql("t", "iceberg_v2")
    assert "'iceberg.enableDeletionVectors' = 'false'" in v2
    delta = _common.create_table_sql("t", "delta")
    assert "delta.feature.catalogManaged" in delta and "USING ICEBERG" not in delta
    assert "CREATE OR REPLACE" not in delta  # catalog commits can't be toggled by REPLACE
    with pytest.raises(ValueError):
        _common.create_table_sql("t", "hudi")


def test_write_result_validates_and_scrubs(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "RESULTS", tmp_path / "r.json")
    with pytest.raises(ValueError):
        _common.write_result("x", question="q", cells={"delta": {"status": "maybe", "finding": ""}}, evidence={})
    _common.write_result(
        "x",
        question="q",
        cells={"delta": {"status": "pass", "finding": "s3://real-bucket-123456789012/a"}},
        evidence={"e": ["randy@example.org"]},
    )
    data = json.loads((tmp_path / "r.json").read_text())
    assert "real-bucket" not in json.dumps(data) and "randy@" not in json.dumps(data)
    assert data["rows"]["x"]["cells"]["delta"]["status"] == "pass"
