from _common import classify, redact

ANTHROPIC_401 = {
    "error_code": "CUSTOMER_UNAUTHORIZED",
    "details": [{"reason": "MODEL_PROVIDER_ERROR",
                 "metadata": {"provider": "anthropic", "upstream_status": "401",
                              "upstream_body": '{"request_id":"req_011abcDEF"}'}}],
}


def test_reached_anthropic_beats_status():
    assert classify(401, ANTHROPIC_401) == "reached_anthropic"


def test_denied_by_uc():
    assert classify(403, {"error_code": "PERMISSION_DENIED", "message": "User does not have EXECUTE"}) == "denied_by_uc"


def test_relay_refused():
    body = {"error_code": "INVALID_PARAMETER_VALUE",
            "message": "This Anthropic Model Provider Service uses relayed authentication and can only be invoked"}
    assert classify(400, body) == "relay_refused"


def test_rate_limited():
    assert classify(429, {"error_code": "REQUEST_LIMIT_EXCEEDED"}) == "rate_limited"


def test_redact_roles_hosts_and_request_ids():
    roles = {"https://ws.example.com": "<workspace>", "me@example.com": "<owner>", "cat.sch": "<schema>"}
    text = "me@example.com called cat.sch.claude_sub_a on https://ws.example.com [ReqId: 5ce4-96ad] req_011abcDEF"
    out = redact(text, roles)
    assert "me@example.com" not in out and "ws.example.com" not in out and "cat.sch" not in out
    assert "5ce4" not in out and "011abc" not in out
    assert "<schema>.claude_sub_a" in out
