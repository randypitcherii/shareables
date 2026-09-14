"""Prove every auth surface with ONE real call each, before any matrix row runs.

- Databricks: SELECT current_user(), current_version() on the warehouse.
- AWS: sts GetCallerIdentity (which identity writes the Glue tables).
- Glue API: GetDatabase on the experiment database.
- Glue Iceberg REST endpoint: GET /v1/config, SigV4-signed. The HTTP status is
  the finding (403 is an authorization result, not a connectivity failure).
- Foreign catalog: SHOW SCHEMAS — proves the connection + credential chain.

Identity is asserted distinct across the two clouds only in the trivial sense
(they are different principals by construction); what matters here is that each
is *proven*, and recorded, so every matrix row can name who measured it.
"""

from _common import (
    aws_identity,
    boto_session,
    dbsql_exec,
    load_config,
    note,
    ok,
    probe_sql,
    section,
    workspace_client,
)


def main() -> None:
    cfg = load_config()

    section("Databricks serverless SQL")
    w = workspace_client(cfg)
    rows = dbsql_exec(cfg, "SELECT current_user(), current_version()", w)
    ok(f"identity={rows[0][0]}  dbsql={rows[0][1]}")

    section("AWS caller")
    ok(f"identity={aws_identity(cfg)}")

    section("Glue API")
    glue = boto_session(cfg).client("glue")
    db = glue.get_database(Name=cfg.glue_database)["Database"]
    ok(f"database={db['Name']} location={db.get('LocationUri')}")

    section("Glue Iceberg REST endpoint (SigV4)")
    import botocore.auth
    import botocore.awsrequest
    import requests

    sess = boto_session(cfg)
    creds = sess.get_credentials().get_frozen_credentials()
    url = f"{cfg.glue_rest_uri}/v1/config"
    req = botocore.awsrequest.AWSRequest(method="GET", url=url, params={"warehouse": cfg.aws_account_id})
    botocore.auth.SigV4Auth(creds, "glue", cfg.aws_region).add_auth(req)
    prepared = req.prepare()
    resp = requests.get(prepared.url, headers=dict(prepared.headers), timeout=30)
    (ok if resp.status_code == 200 else note)(f"GET /v1/config -> HTTP {resp.status_code}")
    note(resp.text[:400])

    section("Foreign catalog")
    out = probe_sql(cfg, f"SHOW SCHEMAS IN {cfg.uc_foreign_catalog}", w)
    if out["ok"]:
        ok(f"schemas={[r[0] for r in out['rows']]}")
    else:
        note(f"{out['error_class']}: {out['error'][:300]}")
        note("run `make setup-uc` if the catalog does not exist yet")

    print("\nverify: all surfaces answered")


if __name__ == "__main__":
    main()
