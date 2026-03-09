from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dev_resource_name import build_dev_resource_name


PROJECT_ID_PATTERN = re.compile(r"^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")


def test_replaces_invalid_username_characters() -> None:
    name = build_dev_resource_name("randy-apps-starter", "randy_pitcher")
    assert name == "randy-pitcher-randy-apps-starter"
    assert PROJECT_ID_PATTERN.fullmatch(name)


def test_prefixes_when_username_starts_with_non_letter() -> None:
    name = build_dev_resource_name("randy-apps-starter", "9-team")
    assert name.startswith("u-9-team-")
    assert PROJECT_ID_PATTERN.fullmatch(name)


def test_truncates_to_lakebase_max_length() -> None:
    name = build_dev_resource_name("randy-apps-starter", "very-long-user-name-" + ("x" * 80))
    assert len(name) <= 63
    assert name.endswith("randy-apps-starter")
    assert PROJECT_ID_PATTERN.fullmatch(name)


def test_respects_shorter_max_len_for_app_names() -> None:
    name = build_dev_resource_name("apps-sbx", "randy_pitcher", max_len=30)
    assert len(name) <= 30
    assert PROJECT_ID_PATTERN.fullmatch(name)
