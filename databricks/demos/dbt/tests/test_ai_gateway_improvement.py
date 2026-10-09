"""Contracts for the AI Gateway improvement loop (models/ai_gateway_improvement/).

No warehouse needed: the rubrics are literal JSON inside macros, the UDF body
is plain Python, the dashboard JSON is generated, and `dbt ls` only parses.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUBRICS = ROOT / "macros" / "ai_gateway_improvement" / "rubrics.sql"
UDF = ROOT / "functions" / "ai_gateway_improvement" / "ai_gateway_distill_transcript.py"
ESCAPE_LABELS = {"unclear", "other"}


# ---- rubrics: the ai_decide `questions` contract ---------------------------

def rubric(kind: str) -> dict:
    text = RUBRICS.read_text()
    match = re.search(
        r"\{% macro ai_gateway_rubric_" + kind + r"\(\) -%\}(.*?)\{%- endmacro %\}", text, re.S
    )
    assert match, f"rubric macro for {kind} not found"
    return json.loads(match.group(1))


@pytest.mark.parametrize("kind", ["metadata", "content"])
def test_rubric_matches_ai_decide_contract(kind: str) -> None:
    questions = rubric(kind)
    assert questions, "a rubric needs at least one question"
    for qid, q in questions.items():
        assert re.fullmatch(r"[a-z][a-z0-9_]*", qid), f"{qid}: ids become SQL column names"
        assert set(q) <= {"type", "instructions", "criteria"}, f"{qid}: unsupported fields"
        assert q["type"] in {"noul", "choice", "score"}, qid
        assert q["instructions"].strip(), qid
        if q["type"] == "score":
            assert 2 <= len(q["criteria"]) <= 10, f"{qid}: score needs 2-10 criteria"
        if q["type"] == "choice":
            labels = set(q["criteria"])
            assert 1 <= len(labels) <= 255, qid
            assert labels & ESCAPE_LABELS, f"{qid}: every choice needs an unclear/other escape"
            # ai_decide (Beta) can return an extra probability for a label
            # literally named `none` and fail the whole call
            assert "none" not in labels, f"{qid}: never use `none` as a label"


@pytest.mark.parametrize("kind", ["metadata", "content"])
def test_rubric_is_safe_in_a_sql_literal(kind: str) -> None:
    body = json.dumps(rubric(kind))
    assert "'" not in body, "rubric JSON is embedded in a single-quoted SQL literal"


def test_downstream_labels_exist_in_the_rubric() -> None:
    friction = set(rubric("content")["primary_friction"]["criteria"])
    recs = (ROOT / "models" / "ai_gateway_improvement" / "ai_gateway__recommendations.sql").read_text()
    for label in re.findall(r"when '([a-z_]+)' then '", recs):
        assert label in friction, f"recommendations reference unknown friction label {label}"
    golden = (ROOT / "seeds" / "ai_gateway_improvement_eval" / "ai_gateway_golden_sessions.csv").read_text()
    outcomes = set(rubric("content")["outcome"]["criteria"])
    goals = set(rubric("content")["goal_category"]["criteria"])
    for line in golden.splitlines()[1:]:
        cells = next(iter(__import__("csv").reader([line])))
        assert cells[5] in outcomes, f"golden {cells[0]}: unknown outcome {cells[5]}"
        assert cells[6] in friction, f"golden {cells[0]}: unknown friction {cells[6]}"
        assert cells[7] in goals, f"golden {cells[0]}: unknown goal {cells[7]}"


# ---- the transcript-distilling UDF ----------------------------------------

def distill():
    body = UDF.read_text()
    assert "$$" not in body, "dbt wraps the UDF body in dollar quotes"
    assert "{{" not in body and "{%" not in body, "dbt renders the body as Jinja first"
    namespace: dict = {}
    exec(  # noqa: S102 -- the file is a UDF body, not a module
        "def f(request, response, api_type, max_chars):\n" + textwrap.indent(body, "    "), namespace
    )
    return lambda req, resp, api="x", limit=12000: json.loads(
        namespace["f"](json.dumps(req) if req is not None else None,
                       json.dumps(resp) if resp is not None else None, api, limit)
    )


def test_distill_anthropic_messages() -> None:
    out = distill()(
        {
            "system": "You are Claude Code",
            "messages": [
                {"role": "user", "content": "fix the test, mail a@b.com, key dapi0123456789abcdef0123456789"},
                {"role": "assistant", "content": [{"type": "text", "text": "ok"}, {"type": "tool_use", "name": "Bash"}]},
                {"role": "user", "content": [
                    {"type": "tool_result", "content": "Error: exit code 1", "is_error": True},
                    {"type": "text", "text": "<system-reminder>ignore me</system-reminder>no, stop"},
                ]},
            ],
        },
        {"content": [{"type": "text", "text": "reverting"}, {"type": "tool_use", "name": "Edit"}]},
    )
    assert out["n_user_turns"] == 2
    assert out["n_tool_calls"] == 2 and out["top_tools"] == ["Bash", "Edit"]
    assert out["n_tool_errors"] == 1
    assert "<email>" in out["first_user_request"] and "<secret>" in out["first_user_request"]
    assert "a@b.com" not in json.dumps(out)
    assert out["recent_user_turns"] == ["no, stop"], "injected reminders are not user text"
    assert out["final_assistant_reply"] == "reverting"


def test_distill_chat_completions() -> None:
    out = distill()(
        {"messages": [
            {"role": "system", "content": "sys"},
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": None, "tool_calls": [{"function": {"name": "search"}}]},
            {"role": "tool", "content": "Traceback: boom"},
            {"role": "user", "content": [{"type": "text", "text": "try again"}]},
        ]},
        {"choices": [{"message": {"content": "done"}}]},
    )
    assert (out["n_user_turns"], out["n_tool_calls"], out["n_tool_errors"]) == (2, 1, 1)
    assert out["final_assistant_reply"] == "done"


def test_distill_openai_responses() -> None:
    out = distill()(
        {"instructions": "codex", "input": [
            {"type": "message", "role": "user", "content": [
                {"type": "input_text", "text": "<environment_context><cwd>/x</cwd></environment_context>add a flag"}]},
            {"type": "function_call", "name": "exec"},
            {"type": "function_call_output", "output": "permission denied"},
        ]},
        {"output": [{"type": "message", "content": [{"type": "output_text", "text": "added"}]}]},
    )
    assert out["first_user_request"] == "add a flag"
    assert (out["n_tool_calls"], out["n_tool_errors"]) == (1, 1)
    assert out["final_assistant_reply"] == "added"


def test_distill_is_bounded_and_never_raises() -> None:
    f = distill()
    long_turns = [{"role": "user", "content": "x" * 5000} for _ in range(40)]
    out = f({"messages": long_turns}, None, limit=6000)
    assert len(json.dumps(out)) <= 6000 + 500
    assert out["truncated"] is True
    assert f(None, None)["n_events"] == 0
    namespace: dict = {}
    exec("def g(request, response, api_type, max_chars):\n" + textwrap.indent(UDF.read_text(), "    "), namespace)  # noqa: S102
    assert json.loads(namespace["g"]("not json", "{", "x", 100))["n_events"] == 0


# ---- dashboard + dbt wiring ------------------------------------------------

def test_dashboard_json_is_generated_and_current() -> None:
    spec = importlib.util.spec_from_file_location("dash", ROOT / "scripts" / "build_ai_gateway_dashboard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    committed = json.loads((ROOT / "dashboards" / "ai_gateway_improvement.lvdash.json").read_text())
    assert committed == module.build(), "run: uv run scripts/build_ai_gateway_dashboard.py"
    models = {p.stem for p in (ROOT / "models" / "ai_gateway_improvement").glob("*.sql")}
    for ds in committed["datasets"]:
        sql = " ".join(ds["queryLines"])
        for table in re.findall(r"\bfrom\s+([a-z_]+)", sql):
            assert table in models, f"dataset {ds['name']} reads unknown table {table}"
        assert "." not in re.findall(r"\bfrom\s+(\S+)", sql)[0], "keep dataset tables unqualified"


def dbt_ls(*selectors: str, environment: str = "development") -> set[str]:
    env = {
        **os.environ,
        "DBT_HOST": "parse-only.invalid",
        "DBT_HTTP_PATH": "/sql/1.0/warehouses/parse-only",
        "DBT_DEPLOYMENT_ENVIRONMENT": environment,
    }
    result = subprocess.run(
        ["dbt", "ls", "--resource-type", "all", "--output", "name", "--quiet", "--no-partial-parse", *selectors],
        cwd=ROOT, env=env, capture_output=True, text=True, check=True,
    )
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def test_hourly_schedule_builds_the_loop_but_never_the_eval() -> None:
    hourly = dbt_ls("-s", "tag:hourly")
    for name in ("stg_ai_gateway__usage", "ai_gateway__session_scores", "ai_gateway__recommendations",
                 "ai_gateway_distill_transcript", "ai_gateway_model_prices"):
        assert name in hourly, f"{name} must run on the hourly schedule"
    assert "ai_gateway__golden_eval" not in hourly
    assert "ai_gateway__golden_eval" not in dbt_ls("-s", "tag:ai_gateway_eval", environment="production")


def test_hourly_models_depend_only_on_hourly_models_or_sources() -> None:
    """dbt Hourly selects tag:hourly alone. A ref() to a daily-only model works
    once dbt Daily has built it, and fails every hour on a fresh deploy until
    then. (This happened: the health mart read int_usage_priced.)"""
    def models(selector: str) -> set[str]:
        env = {**os.environ, "DBT_HOST": "parse-only.invalid", "DBT_HTTP_PATH": "/sql/1.0/warehouses/parse-only",
               "DBT_DEPLOYMENT_ENVIRONMENT": "production"}
        out = subprocess.run(
            ["dbt", "ls", "--resource-type", "model", "--resource-type", "seed", "--resource-type", "function",
             "--output", "name", "--quiet", "--no-partial-parse", "-s", selector],
            cwd=ROOT, env=env, capture_output=True, text=True, check=True,
        ).stdout
        return {line.strip() for line in out.splitlines() if line.strip()}

    hourly = models("tag:hourly")
    upstream = models("+tag:hourly")
    assert upstream <= hourly, f"hourly models ref non-hourly models: {sorted(upstream - hourly)}"
