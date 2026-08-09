"""Deployment defaults for the DEFAULT-profile workspace."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_bundle_and_template_default_to_the_renamed_catalog() -> None:
    bundle = (ROOT / "databricks" / "databricks.yml").read_text()
    template = (ROOT / "template.env").read_text()

    assert "default: rpw_prod" in bundle
    assert "UC_CATALOG=rpw_prod" in template
