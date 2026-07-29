"""No-infra tests for the classifier rows 8, 10, 11 and 14 score on.

Trap 4 in the evidence trail is the reason this file exists: `http_request()`
returns a transport-shaped value for authorization refusals as well as for
transport outcomes, so a Unity Catalog `PERMISSION_DENIED` arrives as
`status_code = 403` inside a statement that *succeeded*. The first version of
row 8 scored on statement success and would have reported the opposite of what
happened.

These cases pin the distinction that mistake turned on: succeeding is not being
allowed, and a non-200 from the origin is not a refusal.
"""

import json

import pytest

import _common
from delegation import _delegation_common


class _FakeClient:
    """Stands in for a WorkspaceClient; only run_sql touches it."""


@pytest.fixture
def run_sql(monkeypatch):
    """Let each test hand the classifier one canned SqlOutcome."""

    def _install(outcome):
        monkeypatch.setattr(
            _delegation_common, "run_sql", lambda w, sql: outcome
        )

    return _install


def _struct(status_code, text):
    return _common.SqlOutcome(True, rows=[[json.dumps({"status_code": status_code, "text": text})]])


UC_REFUSAL = (
    "[REMOTE_FUNCTION_HTTP_FAILED_ERROR] The remote HTTP request failed with code 403, "
    'and error message \'HTTP request failed with status: {"error_code":"PERMISSION_DENIED",'
    '"message":"Failed request to https://example.com:443/. Error: User is missing '
    "USE CONNECTION on some_conn\"}' SQLSTATE: 57012"
)


def test_unity_catalog_refusal_inside_a_successful_statement_is_not_allowed(run_sql):
    """The exact shape that made row 8 report the opposite of the truth."""
    run_sql(_struct("403", UC_REFUSAL))

    result = _delegation_common.http_request_outcome(_FakeClient(), "SELECT ...")

    assert result["statement_succeeded"] is True
    assert result["denied_by_unity_catalog"] is True
    assert result["allowed"] is False


def test_origin_answering_200_is_allowed(run_sql):
    run_sql(_struct("200", "<html>Example Domain</html>"))

    result = _delegation_common.http_request_outcome(_FakeClient(), "SELECT ...")

    assert result["allowed"] is True
    assert result["denied_by_unity_catalog"] is False


@pytest.mark.parametrize("status_code", ["404", "405", "500"])
def test_non_200_from_the_origin_is_not_a_platform_refusal(run_sql, status_code):
    """Row 10 turns on this: a 404 from the far end proves the call was dispatched."""
    run_sql(_struct(status_code, "Not Found"))

    result = _delegation_common.http_request_outcome(_FakeClient(), "SELECT ...")

    assert result["denied_by_unity_catalog"] is False
    assert result["allowed"] is False
    assert result["status_code"] == status_code


def test_statement_level_failure_is_reported_with_its_error(run_sql):
    """Row 10's path-traversal probe never becomes a struct at all."""
    run_sql(
        _common.SqlOutcome(
            False,
            error="[INVALID_HTTP_REQUEST_PATH] ... path traversal is not allowed.",
            error_code="BAD_REQUEST",
        )
    )

    result = _delegation_common.http_request_outcome(_FakeClient(), "SELECT ...")

    assert result["statement_succeeded"] is False
    assert result["allowed"] is False
    assert "INVALID_HTTP_REQUEST_PATH" in result["error"]


def test_long_bodies_are_clipped_before_they_reach_results(run_sql):
    run_sql(_struct("200", "y" * 5000))

    result = _delegation_common.http_request_outcome(_FakeClient(), "SELECT ...")

    assert len(result["body"]) < 5000
    assert result["body"].endswith("...[truncated]")
