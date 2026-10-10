#!/usr/bin/env python3
"""Generate dashboards/s3_storage.lvdash.json.

The S3 Storage dashboard reads the two UC METRIC VIEWS directly -- no SQL
datasets, no copied business logic. Every widget asks a metric view for a
dimension and a MEASURE(), so the dashboard, Genie, and any agent querying the
views all get the same numbers from the same definitions:

    s3_storage_metrics                      -> Overview / Versioning / Tables / Savings / Explore
    s3_inventory_pipeline_health_metrics    -> Pipeline health

Run it after changing the dashboard, then deploy the bundle:

    make s3-storage-dashboard

Dataset sources are UNQUALIFIED metric-view names on purpose. The bundle injects
the catalog and schema per target (dataset_catalog / dataset_schema in
resources/s3_storage.dashboard.yml), so one JSON serves dev and prod.
tests/test_s3_storage_dashboard.py checks every field used here exists in the
metric-view YAML.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "dashboards" / "s3_storage.lvdash.json"

# dataset name -> (display name, metric view)
DATASETS = {
    "storage": ("S3 storage (metric view)", "s3_storage_metrics"),
    "health": ("Inventory pipeline health (metric view)", "s3_inventory_pipeline_health_metrics"),
}


def ds_json() -> list[dict]:
    # A metric-view dataset: the view's own dimensions and measures, nothing redefined.
    return [
        {
            "name": name,
            "displayName": display,
            "config": {
                "version": "1.1",
                "source": view,
                "fields": [{"expr": "source.*"}],
                "measures": [{"expr": "source.*"}],
            },
        }
        for name, (display, view) in DATASETS.items()
    ]


# ---- field helpers: D("Bucket") is a dimension, M("Total Storage") a measure ----

def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def D(name: str) -> dict:
    return {"name": slug(name), "expression": f"`{name}`", "_title": name}


def M(name: str) -> dict:
    return {"name": f"m_{slug(name)}", "expression": f"MEASURE(`{name}`)", "_title": name}


def clean(fields: list[dict]) -> list[dict]:
    return [{"name": f["name"], "expression": f["expression"]} for f in fields]


def query(dataset: str, fields: list[dict], name: str = "main_query") -> list[dict]:
    return [{"name": name, "query": {"datasetName": dataset, "fields": clean(fields), "disaggregated": False}}]


def widget(name: str, queries: list[dict], spec: dict, x: int, y: int, w: int, h: int) -> dict:
    return {"widget": {"name": name, "queries": queries, "spec": spec}, "position": {"x": x, "y": y, "width": w, "height": h}}


def frame(title: str, description: str | None = None) -> dict:
    out = {"title": title, "showTitle": True}
    if description:
        out.update({"description": description, "showDescription": True})
    return out


def counter(name: str, title: str, dataset: str, measure: dict, x: int, y: int, w: int = 1, h: int = 3,
            description: str | None = None) -> dict:
    spec = {
        "version": 2,
        "widgetType": "counter",
        "frame": frame(title, description),
        "encodings": {"value": {"fieldName": measure["name"], "displayName": title}},
    }
    return widget(name, query(dataset, [measure]), spec, x, y, w, h)


def bar(name: str, title: str, dataset: str, dim: dict, measure: dict, x: int, y: int, w: int = 3, h: int = 6,
        color: dict | None = None, horizontal: bool = True, description: str | None = None) -> dict:
    fields = [dim, measure] + ([color] if color else [])
    cat = {"fieldName": dim["name"], "displayName": dim["_title"], "scale": {"type": "categorical"}}
    val = {"fieldName": measure["name"], "displayName": measure["_title"], "scale": {"type": "quantitative"}}
    encodings = {"x": val, "y": cat} if horizontal else {"x": cat, "y": val}
    if color:
        encodings["color"] = {"fieldName": color["name"], "displayName": color["_title"], "scale": {"type": "categorical"}}
    spec = {"version": 3, "widgetType": "bar", "frame": frame(title, description), "encodings": encodings}
    if color:
        spec["mark"] = {"layout": "stack"}
    return widget(name, query(dataset, fields), spec, x, y, w, h)


def line(name: str, title: str, dataset: str, xdim: dict, measure: dict, x: int, y: int, w: int = 3, h: int = 6,
         color: dict | None = None, description: str | None = None) -> dict:
    fields = [xdim, measure] + ([color] if color else [])
    encodings = {
        "x": {"fieldName": xdim["name"], "displayName": xdim["_title"], "scale": {"type": "temporal"}},
        "y": {"fieldName": measure["name"], "displayName": measure["_title"], "scale": {"type": "quantitative"}},
    }
    if color:
        encodings["color"] = {"fieldName": color["name"], "displayName": color["_title"], "scale": {"type": "categorical"}}
    spec = {"version": 3, "widgetType": "line", "frame": frame(title, description), "encodings": encodings}
    return widget(name, query(dataset, fields), spec, x, y, w, h)


def table(name: str, title: str, dataset: str, fields: list[dict], x: int, y: int, w: int = 6, h: int = 8,
          description: str | None = None) -> dict:
    spec = {
        "version": 2,
        "widgetType": "table",
        "frame": frame(title, description),
        "encodings": {"columns": [{"fieldName": f["name"], "displayName": f["_title"], "title": f["_title"]} for f in fields]},
    }
    return widget(name, query(dataset, fields), spec, x, y, w, h)


def multi_filter(name: str, title: str, dataset: str, dim: dict, x: int, y: int, w: int = 2) -> dict:
    qname = f"filter_{name}"
    spec = {
        "version": 2,
        "widgetType": "filter-multi-select",
        "frame": frame(title),
        "encodings": {"fields": [{"fieldName": dim["name"], "displayName": title, "queryName": qname}]},
    }
    return widget(name, query(dataset, [dim], qname), spec, x, y, w, 1)


def text(name: str, md: str, x: int, y: int, w: int = 6, h: int = 2) -> dict:
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}}, "position": {"x": x, "y": y, "width": w, "height": h}}


# ---- pages -------------------------------------------------------------------

S, H = "storage", "health"

PAGES = [
    ("overview", "Overview", [
        text("intro", "## S3 storage at a glance\n"
             "Size, monthly storage cost and estimated savings from each bucket's **latest S3 Inventory snapshot**. "
             "Costs are S3 storage **list prices** (the `s3_storage_class_pricing` seed), not invoiced spend. "
             "Every number comes from the `s3_storage_metrics` metric view. Query it directly or ask Genie for anything not shown here. "
             "Check **Pipeline health** before acting on a number.", 0, 0, 6, 2),
        counter("c_total", "Total storage", S, M("Total Storage"), 0, 2),
        counter("c_cost", "Monthly storage cost", S, M("Monthly Storage Cost"), 1, 2),
        counter("c_savings", "Est. monthly savings", S, M("Estimated Monthly Savings"), 2, 2),
        counter("c_noncurrent", "Noncurrent share", S, M("Noncurrent Share"), 3, 2,
                description="Bytes kept only by bucket versioning"),
        counter("c_buckets", "Buckets", S, M("Buckets"), 4, 2),
        counter("c_tables", "Tables", S, M("Tables"), 5, 2),
        bar("b_bucket", "Storage by bucket", S, D("Bucket"), M("Total Storage"), 0, 5, 3, 6, color=D("Version State")),
        bar("b_cost_class", "Monthly cost by storage class", S, D("Storage Class"), M("Monthly Storage Cost"), 3, 5, 3, 6),
        table("t_buckets", "Buckets", S, [
            D("Bucket"), D("Versioning Status"), D("Is Table Bucket"), M("Total Storage"), M("Noncurrent Storage"),
            M("Current Objects"), M("Monthly Storage Cost"), M("Estimated Monthly Savings"),
        ], 0, 11, 6, 6),
    ]),
    ("versioning", "Versioning", [
        text("v_note", "Versioning status is **inferred** from the inventory's version fields. "
             "`unknown` means the inventory lists current versions only; turn on *Include all versions* to classify it. "
             "On a versioned bucket, every object Delta VACUUM or OPTIMIZE deletes stays as a **noncurrent version** until a lifecycle rule expires it.",
             0, 0, 6, 2),
        counter("v_noncurrent", "Noncurrent storage", S, M("Noncurrent Storage"), 0, 2, 2),
        counter("v_noncurrent_cost", "Noncurrent monthly cost", S, M("Noncurrent Monthly Cost"), 2, 2, 2),
        counter("v_markers", "Delete markers", S, M("Delete Markers"), 4, 2, 2),
        bar("v_by_status", "Storage by versioning status", S, D("Versioning Status"), M("Total Storage"), 0, 5, 3, 6,
            color=D("Version State")),
        bar("v_share", "Noncurrent share by bucket", S, D("Bucket"), M("Noncurrent Share"), 3, 5, 3, 6),
        table("v_buckets", "Buckets by versioning status", S, [
            D("Versioning Status"), D("Bucket"), M("Current Storage"), M("Noncurrent Storage"), M("Noncurrent Share"),
            M("Delete Markers"), M("Noncurrent Monthly Cost"),
        ], 0, 11, 6, 6),
    ]),
    ("tables", "Tables", [
        multi_filter("f_t_bucket", "Bucket", S, D("Bucket"), 0, 0),
        multi_filter("f_t_owner", "Ownership", S, D("Path Source"), 2, 0),
        multi_filter("f_t_activity", "Table activity", S, D("Table Activity"), 4, 0),
        bar("t_activity", "Storage by table activity", S, D("Table Activity"), M("Total Storage"), 0, 1, 3, 6,
            color=D("Path Source"),
            description="inactive = no read or write in table lineage within the activity window"),
        bar("t_owner", "Monthly cost by ownership", S, D("Path Source"), M("Monthly Storage Cost"), 3, 1, 3, 6,
            description="unity_catalog = a UC table owns it; inferred_delta_log = an unregistered Delta table; untracked = no table"),
        table("t_paths", "Storage by table path", S, [
            D("Table Path"), D("Table Name"), D("Path Source"), D("Table Activity"), M("Current Storage"),
            M("Noncurrent Storage"), M("Current Objects"), M("Average Object Size"), M("Monthly Storage Cost"),
            M("Estimated Monthly Savings"),
        ], 0, 7, 6, 10),
    ]),
    ("savings", "Savings", [
        text("s_note", "Each byte counts toward **at most one** opportunity, so savings add up across rows. "
             "Estimates cover storage list price only; they exclude request, transition and early-deletion charges.", 0, 0, 6, 1),
        counter("s_monthly", "Est. monthly savings", S, M("Estimated Monthly Savings"), 0, 1, 2),
        counter("s_annual", "Est. annual savings", S, M("Estimated Annual Savings"), 2, 1, 2),
        counter("s_share", "Savings share of cost", S, M("Savings Share"), 4, 1, 2),
        bar("s_by_opp", "Est. monthly savings by opportunity", S, D("Savings Opportunity"), M("Estimated Monthly Savings"),
            0, 4, 3, 6),
        bar("s_by_bucket", "Est. monthly savings by bucket", S, D("Bucket"), M("Estimated Monthly Savings"), 3, 4, 3, 6,
            color=D("Savings Opportunity")),
        multi_filter("f_s_opp", "Opportunity", S, D("Savings Opportunity"), 0, 10),
        multi_filter("f_s_bucket", "Bucket", S, D("Bucket"), 2, 10),
        table("s_detail", "Where to act (filter Opportunity above; \"none\" = nothing to do)", S, [
            D("Savings Opportunity"), D("Bucket"), D("Table Path"), D("Table Name"), M("Storage With Savings"),
            M("Object Versions"), M("Monthly Storage Cost"), M("Estimated Monthly Savings"), M("Estimated Annual Savings"),
        ], 0, 11, 6, 9),
    ]),
    ("explore", "Explore", [
        text("e_note", "Slice by any dimension in `s3_storage_metrics`: top-level prefix, storage class, object age, or object size.",
             0, 0, 6, 1),
        multi_filter("f_e_bucket", "Bucket", S, D("Bucket"), 0, 1),
        multi_filter("f_e_class", "Storage class", S, D("Storage Class"), 2, 1),
        multi_filter("f_e_kind", "Object kind", S, D("Object Kind"), 4, 1),
        bar("e_age", "Storage by object age", S, D("Object Age Band"), M("Total Storage"), 0, 2, 3, 6,
            color=D("Storage Class"), horizontal=False),
        bar("e_size", "Objects by size band", S, D("Size Band"), M("Current Objects"), 3, 2, 3, 6, horizontal=False,
            description="Many objects under 128 KB means small files: slow scans and minimum-size billing"),
        table("e_prefix", "Top-level prefixes", S, [
            D("Bucket"), D("Top-Level Prefix"), M("Total Storage"), M("Current Objects"), M("Small Objects"),
            M("Average Object Size"), M("Monthly Storage Cost"), M("Estimated Monthly Savings"),
        ], 0, 8, 6, 8),
    ]),
    ("health", "Pipeline health", [
        text("h_note", "## Is the inventory feed healthy?\n"
             "From the `s3_inventory_pipeline_health_metrics` metric view, with one row per bucket per inventory snapshot. "
             "**Stale buckets** and the *hours since* tiles use the current clock. Everything else is checked per snapshot. "
             "Health status shows the first failing check: `missing_ingested_at`, `malformed_rows`, `late_ingest`, `delivery_gap`, "
             "`volume_shift`, `unpriced_storage`. Sample data (`inventory_source = sample`) is always stale.", 0, 0, 6, 2),
        counter("h_stale", "Stale buckets", H, M("Stale Buckets"), 0, 2, description="Latest snapshot older than the gap threshold"),
        counter("h_unhealthy", "Unhealthy snapshots", H, M("Unhealthy Snapshots"), 1, 2),
        counter("h_since_snap", "Hours since latest snapshot", H, M("Hours Since Latest Snapshot"), 2, 2),
        counter("h_since_ingest", "Hours since last ingest", H, M("Hours Since Last Ingest"), 3, 2),
        counter("h_since_build", "Hours since dbt build", H, M("Hours Since dbt Build"), 4, 2),
        counter("h_missing", "Rows missing ingested_at", H, M("Rows Missing ingested_at"), 5, 2,
                description="Must be 0: the build fails otherwise"),
        bar("h_status", "Snapshots by health status", H, D("Bucket"), M("Snapshots"), 0, 5, 3, 6, color=D("Health Status")),
        bar("h_lag", "Max ingest lag by bucket (hours)", H, D("Bucket"), M("Max Ingest Lag Hours"), 3, 5, 3, 6,
            description="Snapshot taken to row ingested (ingested_at)"),
        line("h_rows", "Rows ingested per snapshot", H, D("Snapshot Date"), M("Rows Ingested"), 0, 11, 3, 6, color=D("Bucket")),
        line("h_bytes", "Inventoried storage per snapshot", H, D("Snapshot Date"), M("Inventoried Storage"), 3, 11, 3, 6,
             color=D("Bucket"), description="Sudden swings trip the volume_shift check"),
        multi_filter("f_h_bucket", "Bucket", H, D("Bucket"), 0, 17),
        multi_filter("f_h_status", "Health status", H, D("Health Status"), 2, 17),
        multi_filter("f_h_latest", "Latest snapshot only", H, D("Is Latest Snapshot"), 4, 17),
        table("h_detail", "Snapshot checks", H, [
            D("Bucket"), D("Snapshot"), D("Health Status"), M("Rows Ingested"), M("Inventoried Storage"),
            M("Max Ingest Lag Hours"), M("Max Snapshot Gap Hours"), M("Max Volume Change"), M("Rows Missing ingested_at"),
            M("Unpriced Rows"), M("Last Ingested At"),
        ], 0, 18, 6, 9),
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


def used_fields() -> dict[str, dict[str, set[str]]]:
    """{metric view: {"dimensions": {...}, "measures": {...}}} referenced by the dashboard (for tests)."""
    out: dict[str, dict[str, set[str]]] = {view: {"dimensions": set(), "measures": set()} for _, view in DATASETS.values()}
    for _, _, widgets in PAGES:
        for w in widgets:
            for q in w["widget"].get("queries", []):
                view = DATASETS[q["query"]["datasetName"]][1]
                for f in q["query"]["fields"]:
                    m = re.fullmatch(r"MEASURE\(`(.+)`\)", f["expression"])
                    if m:
                        out[view]["measures"].add(m.group(1))
                    else:
                        out[view]["dimensions"].add(f["expression"].strip("`"))
    return out


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(build(), indent=2) + "\n")
    print(f"wrote {OUT}")
