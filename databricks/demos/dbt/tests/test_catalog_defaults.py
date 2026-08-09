"""Deployment defaults for the DEFAULT-profile workspace."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OLD_CATALOG = "fe_randy_pitcher_workspace_catalog"


def test_active_dbt_sources_use_the_renamed_catalog() -> None:
    bundle = (ROOT / "databricks.yml").read_text()
    active_sources = [
        bundle,
        (ROOT / "docs_app" / "app.py").read_text(),
        (ROOT / "docs_app" / "app.yaml").read_text(),
        (ROOT / "README.md").read_text(),
    ]

    assert "default: rpw_prod" in bundle
    assert all(OLD_CATALOG not in source for source in active_sources)
