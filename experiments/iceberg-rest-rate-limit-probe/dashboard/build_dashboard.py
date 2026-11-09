"""Generate dashboard/iceberg_rest_probe.lvdash.json (AI/BI dashboard).

The checked-in JSON uses the placeholder __FQ__ for `<catalog>`.`<schema>`;
scripts/05_dashboard.py substitutes it at deploy time. Re-run this after editing:
    uv run python dashboard/build_dashboard.py
"""

import json
from pathlib import Path

OUT = Path(__file__).with_name("iceberg_rest_probe.lvdash.json")


def ds(name, display, sql):
    return {"name": name, "displayName": display, "queryLines": [sql]}


def field(name, expr=None):
    return {"name": name, "expression": expr or f"`{name}`"}


def q(dataset, fields, name="main_query", disaggregated=False):
    return {"name": name, "query": {"datasetName": dataset, "fields": fields,
                                     "disaggregated": disaggregated}}


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def text(name, lines, p):
    # the textbox concatenates lines verbatim: end each paragraph with a blank line
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [ln + "\n\n" for ln in lines]}},
            "position": p}


def line(name, title, dataset, x, y, color, ytitle, p, agg="SUM", xtitle=None, xscale="quantitative"):
    yname = f"{agg.lower()}({y})"
    return {"widget": {
        "name": name,
        "queries": [q(dataset, [field(x), field(color), field(yname, f"{agg}(`{y}`)")])],
        "spec": {"version": 3, "widgetType": "line", "encodings": {
            "x": {"fieldName": x, "scale": {"type": xscale}, "displayName": xtitle or x},
            "y": {"fieldName": yname, "scale": {"type": "quantitative"}, "displayName": ytitle},
            "color": {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}},
            "frame": {"showTitle": True, "title": title}}},
        "position": p}


def table(name, title, dataset, cols, p):
    return {"widget": {
        "name": name,
        "queries": [q(dataset, [field(c) for c in cols], disaggregated=True)],
        "spec": {"version": 1, "widgetType": "table", "encodings": {
            "columns": [{"fieldName": c, "displayName": c} for c in cols]},
            "frame": {"showTitle": True, "title": title}}},
        "position": p}


def select_filter(name, title, column, datasets, p, default=None):
    queries = [q(d, [field(column)], name=f"f_{name}_{d}") for d in datasets]
    spec = {"version": 2, "widgetType": "filter-single-select", "encodings": {
        "fields": [{"fieldName": column, "displayName": column, "queryName": qq["name"]}
                   for qq in queries]},
        "frame": {"showTitle": True, "title": title}}
    if default:
        spec["selection"] = {"defaultSelection": {"values": {"dataType": "STRING",
                                                             "values": [{"value": default}]}}}
    return {"widget": {"name": name, "queries": queries, "spec": spec}, "position": p}


FQ = "__FQ__"
datasets = [
    ds("seconds_long", "Probe: per-second series",
       f"SELECT run_id, endpoint, workspace, t, series, value FROM {FQ}.v_probe_seconds_long"),
    ds("seconds", "Probe: per-second latency",
       f"SELECT run_id, endpoint, workspace, t, s.series, s.ms FROM {FQ}.v_probe_seconds "
       "LATERAL VIEW STACK(2, 'p50', p50_ms, 'p95', p95_ms) s AS series, ms"),
    ds("steps", "Probe: 429 share by target rate",
       f"SELECT run_id, endpoint, workspace, target_rate, throttled_share, p50_ms, "
       f"CONCAT(endpoint, ' / ws ', workspace) AS endpoint_ws FROM {FQ}.v_probe_steps"),
    ds("onset", "Probe: onset per endpoint",
       f"SELECT run_id, endpoint, workspace, first_429_rate, onset_rate, peak_target_rate "
       f"FROM {FQ}.v_probe_onset ORDER BY endpoint, workspace"),
    ds("audit", "Audit: Iceberg REST calls per minute",
       f"SELECT minute, CONCAT(service_name, '.', action_name) AS action, "
       f"status_code, calls, peak_qps FROM {FQ}.v_iceberg_rest_audit"),
    ds("audit_status", "Audit: Iceberg REST calls by status",
       f"SELECT minute, CAST(status_code AS STRING) AS status_code, SUM(calls) AS calls "
       f"FROM {FQ}.v_iceberg_rest_audit WHERE service_name = 'uniformIcebergRestCatalog' "
       "GROUP BY ALL"),
    ds("audit_failures", "Audit: Iceberg REST failures",
       f"SELECT event_time, action_name, status_code, error_message, user_agent "
       f"FROM {FQ}.v_iceberg_rest_audit_failures ORDER BY event_time DESC LIMIT 500"),
]

probe_page = [
    text("title", [
        "## Iceberg REST catalog: when do 429s start?",
        "Probe runs against a transient sandbox workspace. Each run ramps one endpoint in steps, "
        "holds above the onset, then drops to a trickle. **Onset** = first step with >= 5% 429s. "
        "One environment, one point in time: compare shapes, don't treat the numbers as an SLA.",
        "**Pick one Run first**: with All selected the charts add every run together."],
        pos(0, 0, 6, 2)),
    select_filter("f_run", "Run", "run_id", ["seconds_long", "seconds", "steps", "onset"],
                  pos(0, 2, 2, 1)),
    select_filter("f_endpoint", "Endpoint", "endpoint", ["seconds_long", "seconds"],
                  pos(2, 2, 2, 1)),
    select_filter("f_ws", "Workspace", "workspace", ["seconds_long", "seconds"],
                  pos(4, 2, 2, 1)),
    line("rate", "Attempted vs succeeded vs 429 per second (pick one endpoint)", "seconds_long",
         "t", "value", "series", "requests / s", pos(0, 3, 6, 6), xtitle="seconds since start"),
    line("latency", "Latency of successful calls (ms)", "seconds", "t", "ms", "series", "ms",
         pos(0, 9, 6, 5), agg="AVG", xtitle="seconds since start"),
    line("share", "429 share by target rate (ramp steps)", "steps", "target_rate",
         "throttled_share", "endpoint_ws", "share of calls rejected", pos(0, 14, 4, 6),
         agg="MAX", xtitle="target req/s per workspace"),
    table("onset_tbl", "Onset per endpoint", "onset",
          ["endpoint", "workspace", "first_429_rate", "onset_rate", "peak_target_rate", "run_id"],
          pos(4, 14, 2, 6)),
]

audit_page = [
    text("audit_title", [
        "## Your real Iceberg REST load (system.access.audit)",
        "Iceberg REST calls log as `uniformIcebergRestCatalog`; the UC calls underneath "
        "(getTable, generateTemporaryTableCredential) log as `unityCatalog`. **Peak QPS** is the "
        "busiest second in each minute: limits are enforced per second, so compare that, not the "
        "average, with the probe's onset. The audit log lagged about 10 minutes in our runs. "
        "429s from the per-workspace edge limit did **not** show up here; 429s from the shared "
        "(cross-workspace) limit did. So a quiet audit log does not prove clients were never throttled."],
        pos(0, 0, 6, 2)),
    line("audit_peak", "Peak calls per second, by action (busiest second in each minute)",
         "audit", "minute", "peak_qps", "action", "peak req/s", pos(0, 2, 6, 6), agg="MAX",
         xtitle="minute", xscale="temporal"),
    line("audit_status_chart", "Iceberg REST calls per minute by status code", "audit_status",
         "minute", "calls", "status_code", "calls / min", pos(0, 8, 6, 5), xtitle="minute",
         xscale="temporal"),
    table("audit_fail", "Recent Iceberg REST failures (4xx/5xx)", "audit_failures",
          ["event_time", "action_name", "status_code", "error_message", "user_agent"],
          pos(0, 13, 6, 6)),
]

dashboard = {
    "datasets": datasets,
    "pages": [
        {"name": "probe", "displayName": "Probe results", "layout": probe_page,
         "pageType": "PAGE_TYPE_CANVAS"},
        {"name": "audit", "displayName": "Real load (audit)", "layout": audit_page,
         "pageType": "PAGE_TYPE_CANVAS"},
    ],
}

if __name__ == "__main__":
    OUT.write_text(json.dumps(dashboard, indent=2) + "\n")
    print(f"wrote {OUT}")
