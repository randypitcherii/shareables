"""Row 10 — Federate to the Glue *Iceberg REST* endpoint instead of Glue-as-HMS.

If Unity Catalog can federate to an Iceberg REST catalog (connection type
ICEBERG_REST), the Glue StorageDescriptor never enters the picture: schema is
resolved from Iceberg metadata, exactly as Athena does (row 7). That would make
the customer's `list<>` tables readable with zero writer changes.

Outcomes this row can record:

  ✅  connection + foreign catalog created, `list<>` table readable
  ❌  created, but the table is still unreadable (would be a very surprising result)
  ⛔  connection type exists but is gated behind a preview the caller cannot
      enable from the API ("Securable kind ... is not enabled") — recorded
      with the exact securable kind so the row can be re-run after enabling
      it in the workspace's Settings → Previews.

Every attempt is recorded verbatim; the row never silently skips.
"""

from _common import (
    load_config,
    note,
    probe_sql,
    record_result,
    section,
    workspace_client,
)
from _matrix_common import identities, write_and_inspect

CONN = "rpw_glue_irc_conn"
CAT = "rpw_glue_irc_cat"


def try_create_connection(cfg, w, label: str, options: dict) -> dict:
    body = {"name": CONN, "connection_type": "ICEBERG_REST", "options": options, "read_only": True}
    try:
        c = w.api_client.do("POST", "/api/2.1/unity-catalog/connections", body=body)
        return {"ok": True, "securable_kind": c.get("securable_kind"), "options": c.get("options")}
    except Exception as e:  # noqa: BLE001 — the message *is* the finding
        msg = str(e)
        return {
            "ok": False,
            "error": msg[:600],
            "gated_securable_kind": _gated_kind(msg),
            "options_sent": {k: ("<redacted>" if "token" in k else v) for k, v in options.items()},
        }


def _gated_kind(msg: str):
    marker = "Securable kind '"
    if marker in msg:
        return msg.split(marker, 1)[1].split("'", 1)[0]
    return None


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    section("row 10: UC federation to the Glue Iceberg REST endpoint (ICEBERG_REST connection)")

    # The reported-failure table — make sure it exists and still carries list<>.
    source = write_and_inspect(cfg, "rest", "list_primitive")
    name = source["table"]

    uri = cfg.glue_rest_uri
    attempts = {
        # SigV4 against Glue via the same self-assuming role UC already trusts.
        "sigv4_iam_role": {"uri": uri, "auth_type": "sigv4", "aws_iam_role_arn": cfg.uc_federation_role_arn,
                           "aws_region": cfg.aws_region, "signing_name": "glue"},
        # The shape of the existing S3 Tables connection in this workspace
        # (CONNECTION_ICEBERG_REST_BEARER_TOKEN + storage_credential_id minted server-side).
        "bearer_token_probe": {"uri": uri, "bearer_token": "probe"},
        # Reuse the UC service credential by name.
        "service_credential_name": {"uri": uri, "credential_name": cfg.uc_service_credential},
    }

    results = {}
    created = None
    for label, opts in attempts.items():
        r = try_create_connection(cfg, w, label, opts)
        results[label] = r
        note(f"{label}: ok={r['ok']} {r.get('gated_securable_kind') or r.get('securable_kind') or ''}")
        if r["ok"]:
            created = label
            break

    catalog_probe = None
    table_read = None
    if created:
        catalog_probe = probe_sql(
            cfg,
            f"CREATE FOREIGN CATALOG IF NOT EXISTS `{CAT}` USING CONNECTION `{CONN}` "
            f"OPTIONS (authorized_paths 's3://{cfg.s3_bucket}/')",
            w,
        )
        note(f"foreign catalog: ok={catalog_probe['ok']} {catalog_probe.get('error_class') or ''}")
        if catalog_probe["ok"]:
            target = f"`{CAT}`.`{cfg.glue_database}`.`{name}`"
            table_read = {
                "describe": probe_sql(cfg, f"DESCRIBE TABLE {target}", w),
                "select_count": probe_sql(cfg, f"SELECT COUNT(*) FROM {target}", w),
            }
            for k, v in table_read.items():
                note(f"{k}: ok={v['ok']} {v.get('error_class') or v.get('rows')}")
            probe_sql(cfg, f"DROP CATALOG IF EXISTS `{CAT}` CASCADE", w)
        try:
            w.api_client.do("DELETE", f"/api/2.1/unity-catalog/connections/{CONN}")
        except Exception:  # noqa: BLE001
            pass

    gated = sorted({r["gated_securable_kind"] for r in results.values() if r.get("gated_securable_kind")})
    readable = bool(table_read and table_read["describe"]["ok"] and table_read["select_count"]["ok"])
    if created:
        status = "✅" if readable else "❌"
    elif gated:
        status = "⛔"
    else:
        status = "❌"

    record_result(
        cfg,
        "10_iceberg_rest_federation",
        {
            "question": "Can UC federate to the Glue Iceberg REST endpoint (ICEBERG_REST connection), "
            "bypassing the Glue SD so list<> tables read?",
            "identity": identities(cfg, w),
            "table": name,
            "glue": source["glue"],
            "endpoint": uri,
            "connection_attempts": results,
            "connection_created_via": created,
            "gated_securable_kinds": gated,
            "foreign_catalog": catalog_probe,
            "uc_read": table_read,
            "readable": readable,
            "blocked_by_workspace_preview": bool(gated and not created),
            "status": status,
        },
    )


if __name__ == "__main__":
    main()
