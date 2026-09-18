"""Row 7 — Is this UC-specific? Ask another Glue reader the same question.

Athena reads Glue Iceberg tables from metadata.json, not the SD. If Athena
returns rows from the list<bigint>/rest table, the SD type string is *not*
load-bearing for Iceberg readers in general — it is a federation-layer choice.
That framing matters for the conversation with the product team.
"""

import time

from _common import boto_session, load_config, note, record_result, section, workspace_client
from _matrix_common import identities, write_and_inspect


def athena(cfg, sql: str) -> dict:
    if not cfg.athena_workgroup:
        return {"ok": False, "error": "ATHENA_WORKGROUP not set (tf output)"}
    client = boto_session(cfg).client("athena")
    qid = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": cfg.glue_database},
        WorkGroup=cfg.athena_workgroup,
    )["QueryExecutionId"]
    for _ in range(60):
        status = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if status["State"] != "SUCCEEDED":
        return {"ok": False, "state": status["State"], "error": status.get("StateChangeReason", "")[:800]}
    rs = client.get_query_results(QueryExecutionId=qid, MaxResults=10)["ResultSet"]["Rows"]
    return {"ok": True, "rows": [[c.get("VarCharValue") for c in r["Data"]] for r in rs]}


def main() -> None:
    cfg = load_config()
    w = workspace_client(cfg)
    section("row 07: Athena reads the list<bigint>/rest table?")
    written = write_and_inspect(cfg, "rest", "list_primitive")
    name = written["table"]
    count = athena(cfg, f'SELECT COUNT(*) FROM "{cfg.glue_database}"."{name}"')
    sample = athena(cfg, f'SELECT id, list_ids FROM "{cfg.glue_database}"."{name}" LIMIT 3')
    describe = athena(cfg, f"DESCRIBE `{cfg.glue_database}`.`{name}`")
    for k, v in {"count": count, "sample": sample, "describe": describe}.items():
        note(f"{k}: {v}")
    record_result(
        cfg,
        "07_athena_cross_reader",
        {
            "question": "Does Athena read the same list<> SD table fine (i.e. is this UC-specific)?",
            "identity": identities(cfg, w),
            **written,
            "athena": {"count": count, "sample": sample, "describe": describe},
            "status": "✅" if (count["ok"] and sample["ok"]) else "❌",
        },
    )


if __name__ == "__main__":
    main()
