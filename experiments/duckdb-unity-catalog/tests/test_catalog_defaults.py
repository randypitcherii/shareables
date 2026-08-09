"""Defaults for scripts targeting the DEFAULT-profile workspace."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_active_scripts_use_the_renamed_catalog() -> None:
    scripts = [
        (ROOT / "scripts" / "_common.py").read_text(),
        (ROOT / "scripts" / "iceberg_rest_eval.py").read_text(),
    ]

    assert all('CATALOG = "rpw_prod"' in script for script in scripts)
