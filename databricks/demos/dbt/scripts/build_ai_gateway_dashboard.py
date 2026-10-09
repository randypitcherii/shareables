#!/usr/bin/env python3
"""Generate dashboards/ai_gateway_improvement.lvdash.json.

The AI/BI dashboard JSON is verbose and hand-edits drift, so it is generated
from this file: datasets as plain SQL, widgets from a few small helpers.
Run it after changing the marts, then deploy the bundle:

    uv run scripts/build_ai_gateway_dashboard.py

Dataset queries use UNQUALIFIED table names on purpose. The bundle injects
the catalog and schema per target (dataset_catalog / dataset_schema in
resources/ai_gateway_improvement.dashboard.yml), so one JSON serves dev and
prod.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "dashboards" / "ai_gateway_improvement.lvdash.json"

DATASETS = {
    "recs": (
        "Recommendations",
        """
        select
          title, detector, category, scope_type, scope_value,
          people_impacted_band, people_impacted, affected_sessions,
          round(est_monthly_savings_usd, 2) as est_monthly_savings_usd,
          impact_score, severity, action, evidence,
          array_join(example_session_ids, ', ') as example_session_ids,
          generated_at
        from ai_gateway__recommendations
        """,
    ),
    "rec_trend": (
        "Recommendation trend",
        """
        select date_trunc('day', snapshot_at) as day, category,
               max(total_savings) as est_monthly_savings_usd
        from (
          select snapshot_at, category, sum(est_monthly_savings_usd) as total_savings
          from ai_gateway__recommendation_history
          group by snapshot_at, category
        )
        group by all
        """,
    ),
    "sessions": (
        "Sessions",
        """
        select
          session_id, started_at, session_date, requester, team, client_family, client_version,
          workload_type_rule as workload_type, invocation_source, primary_model, primary_model_tier,
          n_requests, n_subagents, round(duration_seconds / 60, 1) as duration_minutes,
          input_tokens, output_tokens, cache_hit_ratio, max_input_tokens, error_rate,
          round(est_cost_usd, 4) as est_cost_usd,
          has_payload_logging, has_transcript, is_scored,
          token_efficiency, model_fit, context_pressure, round(thrash_probability, 2) as thrash_probability,
          goal_category, complexity, outcome, quality, user_frustration, primary_friction, prompt_clarity,
          first_user_request
        from ai_gateway__sessions
        """,
    ),
    "daily": (
        "Daily usage",
        """
        select event_date, endpoint_name, client_family, model_tier,
               count(*) as requests,
               sum(case when is_error then 1 else 0 end) as errors,
               sum(case when is_rate_limited then 1 else 0 end) as rate_limited,
               sum(est_cost_usd) as est_cost_usd,
               sum(cache_read_tokens) as cache_read_tokens,
               sum(input_tokens) as input_tokens
        from int_ai_gateway__usage_events
        where service_type != 'MCP_SERVICE'
        group by all
        """,
    ),
    "health": (
        "Measurement health",
        "select metric_group, metric, value, detail, computed_at from ai_gateway__measurement_health",
    ),
}


def ds_json() -> list[dict]:
    return [
        {"name": name, "displayName": display, "queryLines": [" ".join(sql.split())]}
        for name, (display, sql) in DATASETS.items()
    ]


def field(name: str, expr: str | None = None) -> dict:
    return {"name": name, "expression": expr or f"`{name}`"}


def query(dataset: str, fields: list[dict], disaggregated: bool) -> list[dict]:
    return [{"name": "main_query", "query": {"datasetName": dataset, "fields": fields, "disaggregated": disaggregated}}]


def counter(name: str, title: str, dataset: str, expr: str, x: int, y: int, w: int = 2) -> dict:
    fname = f"v_{name}"
    return {
        "widget": {
            "name": name,
            "queries": query(dataset, [field(fname, expr)], False),
            "spec": {
                "version": 2,
                "widgetType": "counter",
                "frame": {"title": title, "showTitle": True},
                "encodings": {"value": {"fieldName": fname, "rowNumber": 0}},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": 3},
    }


def table(name: str, title: str, dataset: str, columns: list[str], x: int, y: int, w: int = 6, h: int = 8) -> dict:
    return {
        "widget": {
            "name": name,
            "queries": query(dataset, [field(c) for c in columns], True),
            "spec": {
                "version": 2,
                "widgetType": "table",
                "frame": {"title": title, "showTitle": True},
                "encodings": {"columns": [{"fieldName": c} for c in columns]},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def bar(name: str, title: str, dataset: str, dim: str, measure: str, expr: str, x: int, y: int,
        w: int = 3, h: int = 6, horizontal: bool = True, color: str | None = None) -> dict:
    fields = [field(dim), field(measure, expr)]
    if color:
        fields.append(field(color))
    cat = {"fieldName": dim, "displayName": dim.replace("_", " "), "scale": {"type": "categorical"}}
    val = {"fieldName": measure, "displayName": title, "scale": {"type": "quantitative"}}
    encodings = {"x": val, "y": cat} if horizontal else {"x": cat, "y": val}
    if color:
        encodings["color"] = {"fieldName": color, "displayName": color.replace("_", " "), "scale": {"type": "categorical"}}
    return {
        "widget": {
            "name": name,
            "queries": query(dataset, fields, False),
            "spec": {"version": 3, "widgetType": "bar", "frame": {"title": title, "showTitle": True}, "encodings": encodings},
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def line(name: str, title: str, dataset: str, xdim: str, measure: str, expr: str, x: int, y: int,
         w: int = 3, h: int = 6, color: str | None = None) -> dict:
    fields = [field(xdim), field(measure, expr)] + ([field(color)] if color else [])
    encodings = {
        "x": {"fieldName": xdim, "displayName": xdim, "scale": {"type": "temporal"}},
        "y": {"fieldName": measure, "displayName": title, "scale": {"type": "quantitative"}},
    }
    if color:
        encodings["color"] = {"fieldName": color, "displayName": color.replace("_", " "), "scale": {"type": "categorical"}}
    return {
        "widget": {
            "name": name,
            "queries": query(dataset, fields, False),
            "spec": {"version": 3, "widgetType": "line", "frame": {"title": title, "showTitle": True}, "encodings": encodings},
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def multi_filter(name: str, title: str, dataset: str, col: str, x: int, y: int, w: int = 2) -> dict:
    qname = f"filter_{name}"
    return {
        "widget": {
            "name": name,
            "queries": [{"name": qname, "query": {"datasetName": dataset, "fields": [field(col)], "disaggregated": False}}],
            "spec": {
                "version": 2,
                "widgetType": "filter-multi-select",
                "frame": {"title": title, "showTitle": True},
                "encodings": {"fields": [{"fieldName": col, "queryName": qname}]},
            },
        },
        "position": {"x": x, "y": y, "width": w, "height": 1},
    }


def text(name: str, md: str, x: int, y: int, w: int = 6, h: int = 2) -> dict:
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}}, "position": {"x": x, "y": y, "width": w, "height": h}}


REC_COLS = ["title", "people_impacted_band", "est_monthly_savings_usd", "impact_score", "action"]

PAGES = [
    ("start", "Start here", [
        text("intro", "## What to fix first\nRanked by **impact score** = dollars at stake (log) + people affected (log) + severity. "
             "Savings are list-price estimates per month. 👤 1 person · 👥 2-9 · 🏢 10+. "
             "Check **Measurement health** before acting on a number.", 0, 0, 6, 2),
        counter("c_savings", "Est. monthly savings (USD)", "recs", "SUM(`est_monthly_savings_usd`)", 0, 2),
        counter("c_recs", "Open recommendations", "recs", "COUNT(`title`)", 2, 2),
        counter("c_sessions", "Sessions (window)", "sessions", "COUNT(`session_id`)", 4, 2),
        table("t_top", "Top recommendations", "recs", REC_COLS, 0, 5, 6, 9),
        bar("b_cat", "Est. monthly savings by category", "recs", "category", "savings",
            "SUM(`est_monthly_savings_usd`)", 0, 14, 3, 6),
        line("l_trend", "Est. monthly savings over time", "rec_trend", "day", "savings",
             "SUM(`est_monthly_savings_usd`)", 3, 14, 3, 6, color="category"),
    ]),
    ("recs", "Recommendations", [
        multi_filter("f_cat", "Category", "recs", "category", 0, 0),
        multi_filter("f_det", "Detector", "recs", "detector", 2, 0),
        multi_filter("f_band", "People impacted", "recs", "people_impacted_band", 4, 0),
        table("t_recs", "All recommendations", "recs",
              ["title", "category", "scope_type", "scope_value", "people_impacted_band", "people_impacted",
               "affected_sessions", "est_monthly_savings_usd", "impact_score", "severity", "action", "evidence",
               "example_session_ids"], 0, 1, 6, 14),
    ]),
    ("sessions", "Sessions explorer", [
        multi_filter("f_client", "Client", "sessions", "client_family", 0, 0),
        multi_filter("f_workload", "Workload", "sessions", "workload_type", 2, 0),
        multi_filter("f_model", "Model", "sessions", "primary_model", 4, 0),
        table("t_sessions", "Sessions (paste an example_session_id into the table search)", "sessions",
              ["session_id", "started_at", "requester", "client_family", "primary_model", "n_requests",
               "duration_minutes", "est_cost_usd", "cache_hit_ratio", "max_input_tokens", "error_rate",
               "token_efficiency", "model_fit", "goal_category", "outcome", "primary_friction",
               "user_frustration", "first_user_request"], 0, 1, 6, 14),
    ]),
    ("efficiency", "Efficiency", [
        bar("b_fit", "Scored sessions by model fit", "sessions", "model_fit", "sessions_n", "COUNT(`session_id`)", 0, 0, 3, 6),
        bar("b_tier", "Est. cost by model tier", "sessions", "primary_model_tier", "cost", "SUM(`est_cost_usd`)", 3, 0, 3, 6),
        bar("b_cache", "Median prompt-cache hit ratio by client", "sessions", "client_family", "cache",
            "MEDIAN(`cache_hit_ratio`)", 0, 6, 3, 7),
        bar("b_eff", "Avg token efficiency (0-4) by client", "sessions", "client_family", "eff",
            "AVG(`token_efficiency`)", 3, 6, 3, 7),
        table("t_ctx", "Largest-context sessions", "sessions",
              ["session_id", "client_family", "primary_model", "max_input_tokens", "n_requests", "context_pressure",
               "est_cost_usd"], 0, 13, 6, 7),
    ]),
    ("quality", "Quality & friction", [
        text("q_note", "Content scores exist only for sessions on model services that log payloads to an inference table. "
             "Coverage is on **Measurement health**.", 0, 0, 6, 1),
        bar("b_friction", "Sessions by primary friction", "sessions", "primary_friction", "n", "COUNT(`session_id`)", 0, 1, 3, 7),
        bar("b_outcome", "Sessions by outcome", "sessions", "outcome", "n", "COUNT(`session_id`)", 3, 1, 3, 7),
        bar("b_goal", "Avg quality (0-4) by task type", "sessions", "goal_category", "q", "AVG(`quality`)", 0, 8, 3, 7),
        bar("b_frus", "Avg frustration (0-3) by client", "sessions", "client_family", "f", "AVG(`user_frustration`)", 3, 8, 3, 7),
        table("t_frus", "Most frustrating sessions", "sessions",
              ["session_id", "requester", "client_family", "goal_category", "outcome", "primary_friction",
               "user_frustration", "first_user_request"], 0, 15, 6, 8),
    ]),
    ("reliability", "Reliability", [
        line("l_err", "Errors per day", "daily", "event_date", "errors", "SUM(`errors`)", 0, 0, 3, 6),
        line("l_429", "Rate-limited (429) per day", "daily", "event_date", "rl", "SUM(`rate_limited`)", 3, 0, 3, 6),
        bar("b_err_ep", "Errors by model service", "daily", "endpoint_name", "e", "SUM(`errors`)", 0, 6, 6, 8),
    ]),
    ("health", "Measurement health", [
        text("h_note", "Is the measurement trustworthy? **coverage** = how much is scored; **confidence** = answers "
             "below the gating threshold (shown as unclear); **stability** = agreement when a session is re-scored; "
             "**anchors** = does a judged score track its deterministic twin; **cost** = estimate vs billing.", 0, 0, 6, 2),
        multi_filter("f_group", "Group", "health", "metric_group", 0, 2),
        table("t_health", "Health metrics", "health", ["metric_group", "metric", "value", "detail", "computed_at"], 0, 3, 6, 14),
    ]),
]


def build() -> dict:
    return {
        "datasets": ds_json(),
        "pages": [
            {"name": name, "displayName": display, "pageType": "PAGE_TYPE_CANVAS", "layout": widgets}
            for name, display, widgets in PAGES
        ],
    }


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(build(), indent=2) + "\n")
    print(f"wrote {OUT}")
