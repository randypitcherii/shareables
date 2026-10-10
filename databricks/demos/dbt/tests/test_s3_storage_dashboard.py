"""Contracts for the S3 Storage dashboard and its metric views.

No warehouse needed: the dashboard JSON is generated, and the metric-view
models are YAML once their Jinja is stripped.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
VIEWS = ROOT / "models" / "metric_views" / "storage"


def load_builder():
    spec = importlib.util.spec_from_file_location("build_s3_storage_dashboard", ROOT / "scripts" / "build_s3_storage_dashboard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def metric_view(name: str) -> dict:
    text = (VIEWS / f"{name}.sql").read_text()
    text = re.sub(r"^\s*(\{\{\s*config\(.*?\)\s*\}\}|\{%-?.*?-?%\})\s*$", "", text, flags=re.M)
    text = re.sub(r"\{\{.*?\}\}", "JINJA", text, flags=re.S)
    return yaml.safe_load(text)


VIEW_NAMES = ["s3_storage_metrics", "s3_inventory_pipeline_health_metrics"]


def test_committed_json_matches_generator() -> None:
    builder = load_builder()
    committed = json.loads((ROOT / "dashboards" / "s3_storage.lvdash.json").read_text())
    assert committed == builder.build(), "run `make s3-storage-dashboard` and commit the result"


def test_every_dataset_is_a_metric_view() -> None:
    builder = load_builder()
    for dataset in builder.build()["datasets"]:
        assert "queryLines" not in dataset, f"{dataset['name']}: datasets must read a metric view, not SQL"
        assert dataset["config"]["source"] in VIEW_NAMES
        # unqualified: the bundle injects catalog + schema per target
        assert "." not in dataset["config"]["source"]


def test_dashboard_fields_exist_in_metric_views() -> None:
    builder = load_builder()
    for view, used in builder.used_fields().items():
        spec = metric_view(view)
        dims = {d["name"] for d in spec["dimensions"]}
        measures = {m["name"] for m in spec["measures"]}
        assert used["dimensions"] <= dims, f"{view}: unknown dimensions {used['dimensions'] - dims}"
        assert used["measures"] <= measures, f"{view}: unknown measures {used['measures'] - measures}"


def test_every_page_has_widgets_and_unique_names() -> None:
    pages = load_builder().build()["pages"]
    assert [p["displayName"] for p in pages][-1] == "Pipeline health"
    names = [w["widget"]["name"] for p in pages for w in p["layout"]]
    assert len(names) == len(set(names)), "widget names must be unique across the dashboard"
    for page in pages:
        assert page["layout"], page["name"]
        for w in page["layout"]:
            pos = w["position"]
            assert pos["x"] + pos["width"] <= 6, f"{w['widget']['name']}: off the 6-column grid"


@pytest.mark.parametrize("view", VIEW_NAMES)
def test_metric_view_metadata_is_agent_ready(view: str) -> None:
    """Agents and Genie lean on this metadata: every field explains itself."""
    spec = metric_view(view)
    assert spec["version"] == 1.1
    assert len(spec["comment"]) > 200, "the view comment should say what it answers and how to use it"
    for field in spec["dimensions"] + spec["measures"]:
        name = field["name"]
        assert "." not in name, f"{name}: dots in names break MEASURE() references"
        assert field.get("display_name"), f"{name}: needs display_name"
        assert len(field.get("comment", "")) >= 20, f"{name}: needs a real comment"
        assert field.get("synonyms"), f"{name}: needs synonyms"
    for measure in spec["measures"]:
        assert measure.get("format"), f"{measure['name']}: measures need a format"
