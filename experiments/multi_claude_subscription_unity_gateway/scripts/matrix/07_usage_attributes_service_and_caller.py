"""Row 07 — does usage attribute each relayed call to the caller AND the subscription's service?

system.ai_gateway.usage carries requester, service_name, status_code and
invocation_metadata.relayed. Ingestion lags 15-40 minutes: run this row again later if it finds
nothing (that is lag, not absence).
"""
import _row  # noqa: F401
from _common import record, schema, section, sql

section("07 usage attributes caller + service")
q = f"""
SELECT service_name, requester, status_code, invocation_metadata.relayed AS relayed, count(*) AS n,
       max(event_time) AS last_seen
FROM system.ai_gateway.usage
WHERE service_name LIKE '{schema()}.claude_sub_%' AND event_time > now() - INTERVAL 3 DAYS
GROUP BY ALL ORDER BY service_name, requester, status_code"""
okq, rows = sql(q)
if not okq:
    record("07_usage_attributes_service_and_caller", "❓", f"usage query failed: {rows}")
elif not rows:
    record("07_usage_attributes_service_and_caller", "❓",
           "no rows yet for these services; usage ingestion lags 15-40 min. Re-run: make row-07")
else:
    services = {r[0] for r in rows}
    requesters = {r[1] for r in rows}
    record("07_usage_attributes_service_and_caller", "✅" if len(services) >= 2 and len(requesters) >= 2 else "◑",
           f"{len(rows)} (service, requester, status) groups across {len(services)} services and "
           f"{len(requesters)} requesters; relayed flag set: {sorted({str(r[3]) for r in rows})}",
           {"rows": rows})
