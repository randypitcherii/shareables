"""No-infra checks on the parts of the harness that turn output into verdicts."""

import json

import install_dbt_v2
import render_readme
import run_matrix


def test_first_error_collapses_block_and_redacts_request_ids():
    output = (
        "noise\n"
        "[error] [DbDriverFailed (dbt1308)]: Database Error in model m\n"
        "  failed: [RequestId=2171a48b-6b9e-4f4f-8d81-fe0668bda7c6 ErrorClass=X] boom\n"
        "[warning] something else\n"
    )
    got = run_matrix.first_error(output)
    assert got.startswith("[error] [DbDriverFailed")
    assert "boom" in got and "RequestId=…" in got and "warning" not in got


def test_nodes_ok_reports_first_failure_and_missing_nodes():
    build = {
        "nodes": {
            "a": {"status": "success", "message": ""},
            "b": {"status": "error", "message": "bad"},
        }
    }
    assert run_matrix.nodes_ok(build, ["a"]) == (True, "")
    assert run_matrix.nodes_ok(build, ["a", "b"])[1].startswith("b: error")
    assert run_matrix.nodes_ok(build, ["c"]) == (False, "c did not run")
    assert run_matrix.nodes_ok(build, [])[0] is False


def test_expect_compares_and_explains():
    assert run_matrix.expect(3, 3, "rows")[0] == "pass"
    status, why = run_matrix.expect(2, 3, "rows")
    assert status == "fail" and "expected 3" in why


def test_row_ids_are_unique():
    ids = [r["id"] for r in run_matrix.ROWS]
    assert len(ids) == len(set(ids))


def test_wheel_url_targets_the_release(monkeypatch):
    monkeypatch.setattr(install_dbt_v2.platform, "system", lambda: "Linux")
    monkeypatch.setattr(install_dbt_v2.platform, "machine", lambda: "aarch64")
    url = install_dbt_v2.wheel_url("2.0.11", "dbt_oss")
    assert url.endswith("/v2.0.11/dbt_oss-2.0.11-cp310-abi3-manylinux_2_28_aarch64.whl")


def test_render_table_orders_local_rows_before_jobs_rows_and_escapes_pipes():
    data = {
        "rows": {
            "j01": {"question": "q", "status": "pass", "evidence": "ok"},
            "r10": {"question": "q", "status": "fail", "evidence": "a | b"},
            "r02": {"question": "q", "status": "partial", "evidence": "x"},
        }
    }
    lines = render_readme.table(data).splitlines()[2:]
    assert [line.split(" | ")[0] for line in lines] == ["| r02", "| r10", "| j01"]
    assert "a \\| b" in lines[1]


def test_committed_results_are_valid():
    path = run_matrix.PROJECT_DIR.parent / "results" / "matrix_results.json"
    if not path.exists():
        return
    data = json.loads(path.read_text())
    for rid, row in data["rows"].items():
        assert row["status"] in ("pass", "fail", "partial"), rid
        assert row["evidence"], rid
