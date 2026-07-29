"""No-infra tests for the helpers that decide what gets recorded and published.

These cover the two failure modes that would silently corrupt the experiment:
recording a status the matrix vocabulary does not define, and writing real
workspace coordinates into a public repo.
"""

import json

import pytest

import _common


def test_status_vocabulary_is_enforced(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "RESULTS_PATH", tmp_path / "matrix_results.json")
    with pytest.raises(ValueError):
        _common.write_result(
            "1", question="q", status="works-i-think", finding="f", evidence={}
        )


def test_write_result_redacts_before_committing(tmp_path, monkeypatch):
    results = tmp_path / "matrix_results.json"
    monkeypatch.setattr(_common, "RESULTS_PATH", results)
    monkeypatch.setattr(
        _common,
        "_redaction_map",
        lambda: {"real_catalog": "my_catalog", "abc-123": "00000000"},
    )

    _common.write_result(
        "1",
        question="does it redact?",
        status="pass",
        finding="caller abc-123 read real_catalog",
        evidence={"error": "no SELECT on real_catalog.t for someone@corp.example"},
    )

    blob = results.read_text()
    for secret in ("real_catalog", "abc-123", "someone@corp.example"):
        assert secret not in blob, f"{secret} survived redaction"
    assert "my_catalog" in blob
    assert _common.PLACEHOLDER_AUTHOR in blob


def test_write_result_round_trips_and_stamps_environment(tmp_path, monkeypatch):
    results = tmp_path / "matrix_results.json"
    monkeypatch.setattr(_common, "RESULTS_PATH", results)
    monkeypatch.setattr(_common, "_redaction_map", dict)

    _common.write_result("1", question="q1", status="pass", finding="f1", evidence={"a": 1})
    _common.write_result("2", question="q2", status="fail", finding="f2", evidence={"b": 2})

    payload = json.loads(results.read_text())
    assert set(payload["rows"]) == {"1", "2"}
    assert payload["rows"]["2"]["status"] == "fail"
    assert payload["environment"]["cloud"] == "AWS"
    assert "recorded_at" in payload["rows"]["1"]


def test_pat_is_refused():
    with pytest.raises(SystemExit):
        _common._refuse_pat("dapiabc123", "DATABRICKS_TOKEN")
    _common._refuse_pat(None, "DATABRICKS_TOKEN")
    _common._refuse_pat("oauth-token", "DATABRICKS_TOKEN")


def test_sql_outcome_summary_truncates_and_exposes_scalar():
    outcome = _common.SqlOutcome(True, rows=[["3"]])
    assert outcome.scalar == "3"
    assert outcome.summary()["succeeded"] is True

    long_error = "x" * 2000
    failed = _common.SqlOutcome(False, error=long_error, error_code="BAD_REQUEST")
    summary = failed.summary()
    assert summary["error_code"] == "BAD_REQUEST"
    assert len(summary["error"]) < len(long_error)
    assert summary["error"].endswith("...[truncated]")
