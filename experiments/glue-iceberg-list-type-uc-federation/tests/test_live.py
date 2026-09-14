"""Infrastructure-marked: one real call per surface. `make test-infrastructure`."""

import pytest

pytestmark = pytest.mark.infrastructure


def test_databricks_identity_resolves():
    from _common import dbsql_identity, load_config

    assert "@" in dbsql_identity(load_config()) or dbsql_identity(load_config())


def test_glue_database_exists():
    from _common import boto_session, load_config

    cfg = load_config()
    db = boto_session(cfg).client("glue").get_database(Name=cfg.glue_database)["Database"]
    assert db["Name"] == cfg.glue_database
