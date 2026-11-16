import pathlib
import re

from rpw_lakewatch import rules

RULES = pathlib.Path(__file__).parents[1] / "rules"
SUBS = {"catalog": "c", "schema": "s", "warehouse_id": "w"}


def test_every_rule_is_prefixed_and_valid_sql_shape():
    specs = rules.load_specs(str(RULES), SUBS)
    assert len(specs) == 7
    for s in specs:
        q = s["batch"]["sql"]["query"]
        assert s["rule_name"].startswith("rpw_")
        assert re.search(r"\bFROM\b", q, re.I) and "IN_TIMERANGE(" in q
        assert "${" not in q, f"unsubstituted placeholder in {s['rule_name']}"


def test_rule_file_name_matches_rule_name():
    for f in RULES.glob("*.yml"):
        assert f"rule_name: {f.stem}\n" in f.read_text()


def test_prepare_disables_by_default_and_tags():
    spec = rules.prepare(rules.load_specs(str(RULES), SUBS)[0], "c", "s", "w", enabled=False)
    assert spec["schedule"]["enabled"] is False and spec["batch"]["warehouse_id"] == "w"
    assert spec["catalog_name"] == "c" and spec["schema_name"] == "s"
    assert spec["annotations"]["rpw.bundle"] == "lakewatch-getting-started"


def test_posture_split():
    specs = rules.load_specs(str(RULES), SUBS)
    posture = [s["rule_name"] for s in specs if rules.uses_posture_tables(s)]
    assert sorted(posture) == ["rpw_posture_control_failing", "rpw_posture_control_regressed"]


def test_unprefixed_rule_rejected(tmp_path):
    (tmp_path / "x.yml").write_text("rule_name: plain\n")
    try:
        rules.load_specs(str(tmp_path), SUBS)
    except ValueError as e:
        assert "rpw_" in str(e)
    else:
        raise AssertionError("expected ValueError")
