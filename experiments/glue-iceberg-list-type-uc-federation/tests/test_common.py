"""No-infrastructure tests: type-token parsing, redaction, results guard.

Everything here runs with zero credentials. Live checks live behind the
`infrastructure` marker in test_live.py.
"""

import json
import os
from pathlib import Path

import pytest

import _common
from _common import Config, error_class, record_result, redact


def _cfg(**over) -> Config:
    base = dict(
        databricks_profile="DEFAULT",
        warehouse_id="wh",
        uc_foreign_catalog="cat",
        uc_connection="conn",
        uc_service_credential="svc",
        uc_storage_credential="stg",
        uc_external_location="loc",
        aws_profile="p",
        aws_region="us-east-1",
        aws_account_id="123456789012",
        s3_bucket="glue-iceberg-list-eval-123456789012-us-east-1",
        glue_database="db",
        uc_federation_role_arn="arn:aws:iam::123456789012:role/glue-iceberg-list-eval-uc-federation",
        athena_workgroup="wg",
        table_prefix="t",
    )
    base.update(over)
    return Config(**base)


def test_error_class_extracts_bracketed_condition():
    msg = "[INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE] Failed to convert Hive table to Spark catalog table."
    assert error_class(msg) == "INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE"


def test_error_class_handles_dotted_conditions_and_absence():
    assert error_class("[UNSUPPORTED_FEATURE.TABLE_OPERATION] nope") == "UNSUPPORTED_FEATURE.TABLE_OPERATION"
    assert error_class("no brackets here") is None
    assert error_class("") is None


def test_redact_replaces_every_real_coordinate():
    cfg = _cfg()
    payload = {
        "path": f"s3://{cfg.s3_bucket}/warehouse/db/t",
        "role": cfg.uc_federation_role_arn,
        "who": "someone@example.com",
        "host": "https://dbc-abc123-def4.cloud.databricks.com/sql",
        "sts": "arn:aws:sts::123456789012:assumed-role/x/y",
        "nested": [{"acct": "123456789012"}],
    }
    out = redact(payload, cfg)
    text = json.dumps(out)
    assert cfg.s3_bucket not in text
    assert "123456789012" not in text
    assert "example.com" not in text
    assert "dbc-abc123-def4" not in text
    assert out["path"] == "s3://<bucket>/warehouse/db/t"
    assert out["role"] == "arn:aws:iam::<aws-account-id>:role/<role>"
    assert out["who"] == "<email>"
    assert out["nested"][0]["acct"] == "<aws-account-id>"


def test_redact_leaves_error_classes_intact():
    cfg = _cfg()
    msg = "[INTERNAL_ERROR_INVALID_HIVE_COLUMN_TYPE] Failed to convert Hive table"
    assert redact(msg, cfg) == msg


def test_record_result_refuses_unproven_identity(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "RESULTS_PATH", tmp_path / "r.json")
    with pytest.raises(SystemExit):
        record_result(_cfg(), "row", {"status": "✅"})
    assert not (tmp_path / "r.json").exists()


def test_record_result_merges_and_redacts(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "RESULTS_PATH", tmp_path / "r.json")
    cfg = _cfg()
    record_result(cfg, "a", {"identity": {"aws": "arn:aws:sts::123456789012:assumed-role/r/s"}, "x": 1})
    record_result(cfg, "b", {"identity": {"databricks": "me@example.com"}, "y": 2})
    data = json.loads((tmp_path / "r.json").read_text())
    assert set(data) == {"a", "b"}
    assert "123456789012" not in json.dumps(data)
    assert data["b"]["identity"]["databricks"] == "<email>"
    assert "recorded_at" in data["a"]


def test_record_result_suffix_keeps_original(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "RESULTS_PATH", tmp_path / "r.json")
    cfg = _cfg()
    record_result(cfg, "a", {"identity": "i", "v": 1})
    monkeypatch.setenv("RESULT_KEY_SUFFIX", "__rerun")
    record_result(cfg, "a", {"identity": "i", "v": 2})
    data = json.loads((tmp_path / "r.json").read_text())
    assert data["a"]["v"] == 1 and data["a__rerun"]["v"] == 2


def test_shapes_literal_matches_builder_registry():
    """SHAPES is a literal so importing _common never imports pyiceberg; keep it in sync."""
    pytest.importorskip("pyiceberg")
    assert set(_common.SHAPES) == set(_common._shapes().keys())


def test_every_shape_builds_a_schema_and_matching_row():
    pytest.importorskip("pyiceberg")
    pa = pytest.importorskip("pyarrow")
    for shape in _common.SHAPES:
        schema = _common.shape_schema(shape)
        row = _common.shape_row(shape, 1)
        tbl = pa.Table.from_pylist([row], schema=schema.as_arrow())
        assert tbl.num_rows == 1, shape


def test_list_shapes_use_list_type():
    pytest.importorskip("pyiceberg")
    from pyiceberg.types import ListType

    s = _common.shape_schema("list_primitive")
    assert isinstance(s.find_field("list_ids").field_type, ListType)


def test_template_env_has_placeholders_only():
    text = (Path(__file__).resolve().parent.parent / "template.env").read_text()
    values = [ln.split("=", 1)[1] for ln in text.splitlines() if "=" in ln and not ln.startswith("#")]
    assert not any(v.startswith("dapi") for v in values), "a PAT-shaped value in the template"
    assert dict(ln.split("=", 1) for ln in text.splitlines() if "=" in ln and not ln.startswith("#"))[
        "AWS_ACCOUNT_ID"
    ] == "000000000000"


def test_glue_sd_types_reads_storage_descriptor():
    table = {"StorageDescriptor": {"Columns": [{"Name": "id", "Type": "bigint"},
                                                {"Name": "list_ids", "Type": "list<bigint>"}]}}
    assert _common.glue_sd_types(table) == {"id": "bigint", "list_ids": "list<bigint>"}


def test_load_config_requires_aws_outputs(monkeypatch):
    for k in ("DATABRICKS_WAREHOUSE_ID", "AWS_REGION", "AWS_ACCOUNT_ID", "S3_BUCKET",
              "GLUE_DATABASE", "UC_FEDERATION_ROLE_ARN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("APP_ENV", "definitely-not-a-real-env")
    with pytest.raises(SystemExit):
        _common.load_config()
    assert "definitely-not-a-real-env" == os.environ["APP_ENV"]
