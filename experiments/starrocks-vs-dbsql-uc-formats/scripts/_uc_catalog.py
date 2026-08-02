"""StarRocks-side helpers for reaching Unity Catalog over the Iceberg REST catalog.

The external catalog is (re)created at run time with a freshly minted OAuth
token from the configured Databricks profile — StarRocks catalog properties are
static strings, so each run rebuilds the catalog inside the token's TTL.
"""

from _common import Config, databricks_config, oauth_token, sr_exec

UC_ICEBERG_CATALOG = "uc_ice"


def _props(cfg: Config, token: str, host: str, style: str) -> dict:
    base = {
        "type": "iceberg",
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": f"{host}/api/2.1/unity-catalog/iceberg-rest",
        "iceberg.catalog.warehouse": cfg.uc_catalog,
    }
    if style == "v4":
        return base | {
            "iceberg.catalog.security": "oauth2",
            "iceberg.catalog.oauth2.token": token,
            "iceberg.catalog.vended-credentials-enabled": "true",
        }
    return base | {
        "iceberg.rest-catalog.security": "OAUTH2",
        "iceberg.rest-catalog.oauth2.token": token,
        "iceberg.rest-catalog.vended-credentials-enabled": "true",
    }


def create_uc_iceberg_catalog(conn, cfg: Config, name: str = UC_ICEBERG_CATALOG) -> dict:
    """(Re)create the UC-backed Iceberg REST catalog; returns which property style worked."""
    host = databricks_config(cfg).host
    token = oauth_token(cfg)
    try:
        sr_exec(conn, f"DROP CATALOG {name}")
    except Exception:  # noqa: BLE001 — absent catalog is fine
        pass
    errors = {}
    for style in ("v4", "legacy"):
        props = ",\n  ".join(f'"{k}" = "{v}"' for k, v in _props(cfg, token, host, style).items())
        ddl = f"CREATE EXTERNAL CATALOG {name} PROPERTIES (\n  {props}\n)"
        try:
            sr_exec(conn, ddl)
            # Prove the catalog actually resolves against UC, not just parses.
            sr_exec(conn, f"SHOW DATABASES FROM {name}")
            return {"catalog": name, "property_style": style, "uri_host": host}
        except Exception as e:  # noqa: BLE001
            errors[style] = str(e)[:500]
            try:
                sr_exec(conn, f"DROP CATALOG {name}")
            except Exception:  # noqa: BLE001
                pass
    raise RuntimeError(f"could not create UC Iceberg REST catalog: {errors}")


def sr_identity(conn) -> dict:
    user, _ = sr_exec(conn, "SELECT current_user()")
    version, _ = sr_exec(conn, "SELECT current_version()")
    return {"user": user[0][0], "version": version[0][0]}
