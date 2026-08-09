"""Deployment defaults for the DEFAULT-profile workspace."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_bundle_defaults_to_the_renamed_catalog() -> None:
    bundle = (ROOT / "databricks.yml").read_text()

    assert "default: rpw_prod" in bundle
    assert "fe_randy_pitcher_workspace_catalog" not in bundle
