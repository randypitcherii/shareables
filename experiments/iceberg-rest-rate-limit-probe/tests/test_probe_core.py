import pytest
from _common import scrub
from probe_core import ProbePlan, Target, analyze, build_request, default_rates, per_second

T = Target(host="https://h", catalog="cat", schema="sch", tables=("t000", "t001", "t002"),
           prefix="catalogs/cat")


def test_build_request_paths():
    assert build_request("config", T, 0)[0] == (
        "https://h/api/2.1/unity-catalog/iceberg-rest/v1/config?warehouse=cat")
    assert build_request("list_tables", T, 0)[0].endswith("/v1/catalogs/cat/namespaces/sch/tables")
    url, hdr = build_request("load_table_spread", T, 4)
    assert url.endswith("/tables/t001") and hdr["X-Iceberg-Access-Delegation"]
    assert build_request("load_table_hot", T, 4)[0].endswith("/tables/t000")
    assert build_request("load_table_nocreds", T, 2)[1] == {}
    legacy, _ = build_request("legacy_load_table", T, 0)
    assert "/unity-catalog/iceberg/v1/catalogs/cat/" in legacy and legacy.endswith("uniform_t000")
    with pytest.raises(ValueError):
        build_request("nope", T, 0)


def test_plan_bounds():
    with pytest.raises(ValueError):
        ProbePlan(rates=[10, 2000], max_rate=1000)
    with pytest.raises(ValueError):
        ProbePlan(rates=[10] * 100, step_sec=20, max_duration_sec=900)
    for e in ("config", "load_table_spread", "legacy_config"):
        ProbePlan(rates=default_rates(e))  # defaults fit inside the default caps


def _rec(t, rate, status, phase="ramp", ra=None):
    return {"t_sent": t, "target_rate": rate, "status": status, "phase": phase,
            "latency_ms": 10.0, "retry_after": ra, "error_code": "X" if status == 429 else None}


def test_analyze_onset_ceiling_recovery():
    recs = []
    t = 0.0
    for rate, s429 in ((10, 0), (20, 1), (40, 20)):  # 40 calls per step
        for i in range(40):
            recs.append(_rec(t + i * 0.025, rate, 429 if i < s429 else 200))
        t += 1.0
    for i in range(50):  # hold: 50 calls in 1s, 25 accepted
        recs.append(_rec(10 + i * 0.02, 50, 200 if i % 2 == 0 else 429, "hold", ra="1"))
    recs.append(_rec(20.0, 2, 429, "recovery", ra="1"))
    recs.append(_rec(20.6, 2, 200, "recovery"))
    recs.append(_rec(21.1, 2, 200, "recovery"))
    a = analyze(recs)
    assert a["first_429_rate"] == 20
    assert a["onset_rate"] == 40
    assert a["accepted_ceiling_rps"] == 25
    assert a["throttle_after_drop_sec"] == 0.0
    assert a["recovery_sec"] == 1.0
    assert a["retry_after"]["values_seen"] == ["1"]


def test_per_second_fills_gaps():
    rows = per_second([_rec(0.1, 1, 200), _rec(2.5, 1, 429)], 0.0)
    assert [r["t"] for r in rows] == [0, 1, 2]
    assert rows[2]["throttled"] == 1 and rows[1]["attempted"] == 0


def test_scrub():
    s = scrub("https://dbc-12ab.cloud.databricks.com/x id 1234567890123456 "
              "a@b.com 0a1b2c3d-1111-2222-3333-444455556666 mycat.sch", extra=("mycat",))
    assert "databricks.com" not in s and "1234567890123456" not in s
    assert "a@b.com" not in s and "mycat" not in s and "<uuid>" in s
