"""Contracts for the demo folders (models/patterns/, models/ai_ml_inference/).

These run without a warehouse: `dbt ls` only parses the project, and the UDF
body is plain Python.
"""

import csv
import json
import os
import re
import subprocess
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
GATED = {
    "ai_ml_inference_embeddings_serving": "DBT_AI_ML_INFERENCE_SERVING_ENDPOINT",
    "assert_ai_ml_inference_paths_agree": "DBT_AI_ML_INFERENCE_SERVING_ENDPOINT",
}
DEMO_TAGS = ("patterns", "ai_ml_inference")


def dbt_ls(*selectors: str, **env_overrides: str) -> list[dict]:
    """Parse the project with placeholder connection details and list resources."""
    env = {
        **os.environ,
        # parse-only: the connection is never opened, but profiles.yml needs values
        "DBT_HOST": "parse-only.invalid",
        "DBT_HTTP_PATH": "/sql/1.0/warehouses/parse-only",
        "DBT_DEPLOYMENT_ENVIRONMENT": "development",
        **{name: "" for name in GATED.values()},
        **env_overrides,
    }
    result = subprocess.run(
        [
            "dbt",
            "ls",
            "--resource-type",
            "all",
            "--output",
            "json",
            "--quiet",
            "--no-partial-parse",
            *selectors,
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return [
        json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")
    ]


@pytest.fixture(scope="module")
def patterns() -> list[dict]:
    return dbt_ls("-s", "tag:patterns")


def test_patterns_select_the_whole_folder(patterns: list[dict]) -> None:
    names = {r["name"] for r in patterns}
    assert {
        "synth_audience_members",
        "audience_features",
        "normalize_region_code",
        "ref_iana_timezones",
    } <= names


@pytest.fixture(scope="module")
def ai_ml_inference() -> list[dict]:
    return dbt_ls("-s", "tag:ai_ml_inference")


def test_ai_ml_inference_selects_its_folder(ai_ml_inference: list[dict]) -> None:
    names = {r["name"] for r in ai_ml_inference}
    assert {"ai_ml_inference_input", "ai_ml_inference_embeddings_python"} <= names
    # the demo is self-contained: no pattern model rides along
    assert not [r for r in ai_ml_inference if "patterns" in r.get("tags", [])]


def test_production_schedule_never_runs_demos() -> None:
    # tags are additive, so a project-level `daily` would leak into every folder
    daily = dbt_ls("-s", "tag:daily")
    assert daily, "the daily selector should still select the production models"
    leaked = [r["unique_id"] for r in daily if set(DEMO_TAGS) & set(r.get("tags", []))]
    assert leaked == []


def test_demos_are_disabled_in_production() -> None:
    # CD builds every MODIFIED node in production, whatever its tags
    everything = dbt_ls(DBT_DEPLOYMENT_ENVIRONMENT="production")
    assert everything, "production should still parse the real models"
    leaked = [r["unique_id"] for r in everything if set(DEMO_TAGS) & set(r.get("tags", []))]
    assert leaked == []


def test_gated_nodes_are_disabled_by_default(ai_ml_inference: list[dict]) -> None:
    names = {r["name"] for r in ai_ml_inference}
    assert names.isdisjoint(GATED)


@pytest.mark.parametrize(("node", "env_var"), GATED.items())
def test_gated_node_enables_when_configured(node: str, env_var: str) -> None:
    names = {r["name"] for r in dbt_ls("-s", node, **{env_var: "some-endpoint"})}
    assert node in names


def test_python_model_and_endpoint_apply_the_same_model_version() -> None:
    project = (ROOT / "dbt_project.yml").read_text()
    spec = json.loads((ROOT / "models" / "ai_ml_inference" / "serving_endpoint.json").read_text())
    (entity,) = spec["config"]["served_entities"]
    assert f"'DBT_AI_ML_INFERENCE_MODEL_NAME', '{entity['entity_name']}'" in project
    assert f"'DBT_AI_ML_INFERENCE_MODEL_VERSION', '{entity['entity_version']}'" in project
    assert entity["scale_to_zero_enabled"] is True


def test_nothing_trains_or_registers_a_model() -> None:
    # the demo APPLIES a logged model; model lifecycle lives outside this repo
    forbidden = re.compile(r"\.fit\(|log_model|register_model|registered_model_alias")
    offenders = [
        str(path.relative_to(ROOT))
        for folder in ("models", "scripts", "macros")
        for path in (ROOT / folder).rglob("*")
        if path.suffix in {".py", ".sql"} and forbidden.search(path.read_text())
    ]
    assert offenders == []


def load_normalize_region_code():
    """Wrap the UDF body in a function, the same way Unity Catalog does."""
    body = (ROOT / "functions" / "patterns" / "normalize_region_code.py").read_text()
    assert "$$" not in body, "a dollar-quote in the body ends the CREATE FUNCTION early"
    assert "{{" not in body and "{%" not in body, "dbt renders the body as Jinja"
    namespace: dict = {}
    exec("def normalize_region_code(raw):\n" + textwrap.indent(body, "    "), namespace)  # noqa: S102
    return namespace["normalize_region_code"]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("US", "US"),
        ("us ", "US"),
        ("USA", "US"),
        ("UK", "GB"),
        ("Germany", "DE"),
        ("ZZ", "ZZ"),
        ("??", None),
        (None, None),
    ],
)
def test_normalize_region_code(raw, expected) -> None:
    assert load_normalize_region_code()(raw) == expected


def read_seed(name: str) -> list[dict]:
    with (ROOT / "seeds" / "patterns" / f"{name}.csv").open() as handle:
        return list(csv.DictReader(handle))


def test_region_seed_is_iso_alpha2() -> None:
    codes = [row["region_code"] for row in read_seed("ref_iso_region_codes")]
    assert len(codes) == len(set(codes)) >= 240
    assert all(re.fullmatch(r"[A-Z]{2}", code) for code in codes)
    assert "GB" in codes and "UK" not in codes


def test_timezone_seed_is_area_location() -> None:
    zones = [row["timezone"] for row in read_seed("ref_iana_timezones")]
    assert len(zones) == len(set(zones)) >= 400
    assert "UTC" in zones and "America/New_York" in zones
    assert not [z for z in zones if z != "UTC" and "/" not in z]
    assert not [z for z in zones if z.startswith(("Etc/", "posix/", "right/"))]
