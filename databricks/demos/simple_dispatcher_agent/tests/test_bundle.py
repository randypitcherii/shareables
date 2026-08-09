"""Deployment defaults for the DEFAULT-profile workspace."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_bundle_defaults_to_the_renamed_catalog() -> None:
    bundle = (ROOT / "databricks.yml").read_text()

    assert "default: rpw_prod" in bundle
    assert "fe_randy_pitcher_workspace_catalog" not in bundle


def test_experiment_names_rotate_with_catalog_renames() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    experiment_lines = [
        line
        for line in bundle.splitlines()
        if line.lstrip().startswith("target_experiment:") and '"' in line
    ]

    assert len(experiment_lines) == 3
    assert all("${var.base_catalog}" in line for line in experiment_lines)
