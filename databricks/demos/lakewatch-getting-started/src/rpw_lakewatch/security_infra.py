"""Databricks security infrastructure checks.

Evaluates six essential workspace security controls plus credential-hygiene
checks and appends one row per (workspace, control) to
<catalog>.<schema>.rpw_security_infra_findings.

Two evidence sources, recorded in the `source` column:
  api           authoritative settings for the workspace this job runs in (needs workspace admin)
  system_table  account-wide evidence from system tables (billing, compute, access) for every workspace

Status values: PASS | PARTIAL | FAIL | UNKNOWN | NOT_APPLICABLE
"""
from __future__ import annotations

import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from .errors import SetupError

ESSENTIAL = {
    "restricted_inbound": ("Restricted Inbound", "network"),
    "restricted_outbound_serverless": ("Restricted Outbound (Serverless)", "network"),
    "no_public_ip": ("No Public IP", "network"),
    "unity_catalog": ("Unity Catalog", "governance"),
    "restrict_workspace_admins": ("Restrict Workspace Admins", "governance"),
    "cluster_restart": ("Cluster Restart (Patch Hygiene)", "compliance"),
}
HYGIENE = {
    "pat_max_lifetime": ("PAT maximum lifetime enforced", "iam"),
    "pat_long_lived_tokens": ("No long-lived / non-expiring PATs", "iam"),
    "sp_workspace_admins": ("Service principals holding workspace admin", "iam"),
    "verbose_audit_logs": ("Verbose audit logging", "compliance"),
}
def _schema():
    from pyspark.sql import types as T
    return T.StructType([
        T.StructField("snapshot_ts", T.TimestampType(), False),
        T.StructField("workspace_id", T.StringType(), False),
        T.StructField("control_key", T.StringType(), False),
        T.StructField("control_name", T.StringType(), False),
        T.StructField("domain", T.StringType(), False),
        T.StructField("tier", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("adoption_pct", T.DoubleType(), True),
        T.StructField("source", T.StringType(), False),
            T.StructField("detail", T.StringType(), True),
    ])
UC_MODES = ("USER_ISOLATION", "SINGLE_USER", "DATA_SECURITY_MODE_STANDARD", "DATA_SECURITY_MODE_DEDICATED", "DATA_SECURITY_MODE_AUTO")


class Collector:
    def __init__(self, spark, w, args):
        self.spark, self.w, self.a = spark, w, args
        self.ts = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        self.rows = []
        self.ws_id = str(w.get_workspace_id())

    def add(self, ws, key, status, pct=None, source="api", **detail):
        name, domain = ESSENTIAL.get(key) or HYGIENE[key]
        tier = "essential" if key in ESSENTIAL else "credential_hygiene"
        self.rows.append((self.ts, str(ws), key, name, domain, tier, status,
                          None if pct is None else float(pct), source, json.dumps(detail, default=str)))

    def guard(self, key, fn, source="api"):
        try:
            fn()
        except Exception as e:  # never let one check kill the snapshot
            self.add(self.ws_id, key, "UNKNOWN", source=source, error=f"{type(e).__name__}: {e}"[:500])

    def sql(self, q):
        return self.spark.sql(q).collect()

    # ---------- this workspace (API) ----------
    def inbound_api(self):
        conf = self.w.workspace_conf.get_status(keys="enableIpAccessLists")
        enabled = str(conf.get("enableIpAccessLists", "false")).lower() == "true"
        allow = [l for l in (self.w.ip_access_lists.list() or []) if l.enabled and str(l.list_type).endswith("ALLOW")]
        status = "PASS" if enabled and allow else "FAIL"
        self.add(self.ws_id, "restricted_inbound", status, 100.0 if status == "PASS" else 0.0,
                 ip_access_lists_enabled=enabled, allow_lists=[l.label for l in allow],
                 note="Front-end Private Link and context-based ingress also satisfy this control; verify in the account console if FAIL.")

    def restrict_admins_api(self):
        s = self.w.settings.restrict_workspace_admins.get()
        st = str(s.restrict_workspace_admins.status)
        ok = st.endswith("RESTRICT_TOKENS_AND_JOB_RUN_AS")
        self.add(self.ws_id, "restrict_workspace_admins", "PASS" if ok else "FAIL", 100.0 if ok else 0.0, setting=st)

    def no_public_ip_api(self):
        if not self.w.config.is_azure:
            self.add(self.ws_id, "no_public_ip", "NOT_APPLICABLE", note="Essential applies to Azure Databricks; AWS/GCP use secure cluster connectivity by default.")
            return
        self.add(self.ws_id, "no_public_ip", "UNKNOWN",
                 note="Check the workspace's enableNoPublicIp parameter in Azure (az databricks workspace show) or the account console.")

    def restart_api(self):
        days = self.a.long_running_days
        now_ms = dt.datetime.now(dt.timezone.utc).timestamp() * 1000
        running = [c for c in self.w.clusters.list() if str(c.state).endswith("RUNNING")]
        stale = []
        for c in running:
            started = c.last_restarted_time or c.start_time
            if started and (now_ms - started) / 86400000 > days:
                stale.append({"cluster_id": c.cluster_id, "name": c.cluster_name, "up_days": round((now_ms - started) / 86400000, 1)})
        pct = 100.0 if not running else 100.0 * (len(running) - len(stale)) / len(running)
        self.add(self.ws_id, "cluster_restart", "PASS" if not stale else "FAIL", pct,
                 running_clusters=len(running), long_running=stale[:25], threshold_days=days)

    def pat_api(self):
        conf = self.w.workspace_conf.get_status(keys="maxTokenLifetimeDays,enableVerboseAuditLogs")
        max_days = conf.get("maxTokenLifetimeDays")
        ok = bool(max_days) and 0 < int(max_days) <= self.a.max_token_days
        self.add(self.ws_id, "pat_max_lifetime", "PASS" if ok else "FAIL", max_token_lifetime_days=max_days)
        verbose = str(conf.get("enableVerboseAuditLogs", "false")).lower() == "true"
        self.add(self.ws_id, "verbose_audit_logs", "PASS" if verbose else "FAIL")

    def tokens_api(self):
        now_ms = dt.datetime.now(dt.timezone.utc).timestamp() * 1000
        toks = list(self.w.token_management.list())
        bad = [t for t in toks if (t.expiry_time is None or t.expiry_time <= 0)
               or (t.expiry_time - (t.creation_time or now_ms)) / 86400000 > self.a.max_token_days]
        pct = 100.0 if not toks else 100.0 * (len(toks) - len(bad)) / len(toks)
        self.add(self.ws_id, "pat_long_lived_tokens", "PASS" if not bad else "FAIL", pct, active_tokens=len(toks),
                 long_lived_or_non_expiring=[{"owner": t.created_by_username, "comment": t.comment, "token_id": t.token_id} for t in bad[:50]])

    def sp_admins_api(self):
        admins = next(iter(self.w.groups.list(filter='displayName eq "admins"', attributes="members")), None)
        if admins is None or admins.members is None:
            self.add(self.ws_id, "sp_workspace_admins", "UNKNOWN", note="admins group membership not visible; run the job as a workspace admin.")
            return
        members = admins.members
        sps = [m.display for m in members if "ServicePrincipals" in (m.ref or "")]
        self.add(self.ws_id, "sp_workspace_admins", "PASS" if not sps else "PARTIAL",
                 service_principals=sps, note="Least privilege: separate deploy and runtime identities; remove admin where not needed.")

    # ---------- account-wide (system tables) ----------
    def unity_catalog_sys(self):
        rows = self.sql(f"""
            WITH c AS (SELECT workspace_id, cluster_id, data_security_mode FROM system.compute.clusters
                       QUALIFY row_number() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) = 1),
                 u AS (SELECT workspace_id, usage_metadata.cluster_id AS cluster_id, product_features.is_serverless AS srv, usage_quantity AS q
                       FROM system.billing.usage WHERE usage_date >= current_date() - 30 AND usage_unit = 'DBU')
            SELECT u.workspace_id, CAST(sum(q) AS DOUBLE) AS total,
                   CAST(sum(CASE WHEN u.cluster_id IS NULL OR u.srv OR c.data_security_mode IN {UC_MODES} THEN q ELSE 0 END) AS DOUBLE) AS uc_q,
                   count(DISTINCT CASE WHEN c.data_security_mode NOT IN {UC_MODES} THEN u.cluster_id END) AS legacy_clusters
            FROM u LEFT JOIN c ON u.workspace_id = c.workspace_id AND u.cluster_id = c.cluster_id
            GROUP BY u.workspace_id""")
        for r in rows:
            pct = 100.0 * (r.uc_q or 0) / r.total if r.total else None
            st = "UNKNOWN" if pct is None else "PASS" if pct >= 95 else "PARTIAL" if pct >= 60 else "FAIL"
            self.add(r.workspace_id, "unity_catalog", st, pct, source="system_table", legacy_mode_clusters=r.legacy_clusters,
                     basis="share of last-30-day DBUs on UC-capable access modes (serverless/SQL counted as UC)")

    def cluster_restart_sys(self):
        d = self.a.long_running_days
        rows = self.sql(f"""
            WITH live AS (SELECT DISTINCT workspace_id, cluster_id, instance_id FROM system.compute.node_timeline
                          WHERE start_time >= current_timestamp() - INTERVAL 1 DAY),
                 age AS (SELECT n.workspace_id, n.cluster_id, n.instance_id, min(n.start_time) AS first_seen
                         FROM system.compute.node_timeline n JOIN live USING (workspace_id, cluster_id, instance_id)
                         WHERE n.start_time >= current_timestamp() - INTERVAL {2 * d} DAYS GROUP BY ALL)
            SELECT workspace_id, count(DISTINCT cluster_id) AS live_clusters,
                   count(DISTINCT CASE WHEN first_seen < current_timestamp() - INTERVAL {d} DAYS THEN cluster_id END) AS long_running
            FROM age GROUP BY workspace_id""")
        for r in rows:
            if r.workspace_id == self.ws_id:
                continue  # API result is authoritative for this workspace
            pct = 100.0 * (r.live_clusters - r.long_running) / r.live_clusters if r.live_clusters else 100.0
            self.add(r.workspace_id, "cluster_restart", "PASS" if r.long_running == 0 else "FAIL", pct, source="system_table",
                     live_clusters=r.live_clusters, long_running_clusters=r.long_running, threshold_days=d)

    def outbound_sys(self):
        rows = self.sql("""
            WITH sv AS (SELECT workspace_id, CAST(sum(usage_quantity) AS DOUBLE) AS dbus FROM system.billing.usage
                        WHERE usage_date >= current_date() - 30 AND product_features.is_serverless GROUP BY ALL),
                 ev AS (SELECT workspace_id, count(*) AS events, count_if(upper(access_type) LIKE '%DRY%') AS dry_run
                        FROM system.access.outbound_network WHERE event_time >= current_timestamp() - INTERVAL 30 DAYS GROUP BY ALL)
            SELECT sv.workspace_id, sv.dbus, coalesce(ev.events, 0) AS events, coalesce(ev.dry_run, 0) AS dry_run
            FROM sv LEFT JOIN ev USING (workspace_id)""")
        for r in rows:
            if r.events and r.dry_run < r.events:
                st, pct = "PASS", 100.0
            elif r.events:
                st, pct = "PARTIAL", 50.0
            else:
                st, pct = "UNKNOWN", None
            self.add(r.workspace_id, "restricted_outbound_serverless", st, pct, source="system_table", serverless_dbus_30d=r.dbus,
                     egress_policy_events_30d=r.events, dry_run_events_30d=r.dry_run,
                     note="Enforced policies log denials to system.access.outbound_network. No events usually means FULL_ACCESS; confirm the network policy in the account console.")

    def inbound_sys(self):
        rows = self.sql("""
            SELECT workspace_id, count(*) AS denied FROM system.access.audit
            WHERE event_date >= current_date() - 30 AND action_name = 'IpAccessDenied' GROUP BY ALL""")
        for r in rows:
            if r.workspace_id == self.ws_id:
                continue
            self.add(r.workspace_id, "restricted_inbound", "PASS", 100.0, source="system_table", ip_access_denied_30d=r.denied,
                     basis="IP access list is enforcing (denials observed)")

    def restrict_admins_sys(self):
        rows = self.sql("""
            SELECT request_params.settingKeyName AS workspace_id, action_name, request_params.settingValueForAudit AS v
            FROM system.access.audit
            WHERE event_date >= current_date() - 365 AND action_name IN ('setSetting', 'deleteSetting')
              AND request_params.settingTypeName ILIKE '%restrict_workspace_admins%'
            QUALIFY row_number() OVER (PARTITION BY request_params.settingKeyName ORDER BY event_time DESC) = 1""")
        for r in rows:
            if r.workspace_id == self.ws_id:
                continue
            ok = r.action_name == "setSetting" and "RESTRICT_TOKENS_AND_JOB_RUN_AS" in (r.v or "")
            self.add(r.workspace_id, "restrict_workspace_admins", "PASS" if ok else "FAIL", 100.0 if ok else 0.0,
                     source="system_table", last_change=r.action_name)

    def run(self):
        api = [("restricted_inbound", self.inbound_api), ("restrict_workspace_admins", self.restrict_admins_api),
               ("no_public_ip", self.no_public_ip_api), ("cluster_restart", self.restart_api),
               ("pat_max_lifetime", self.pat_api), ("pat_long_lived_tokens", self.tokens_api),
               ("sp_workspace_admins", self.sp_admins_api)]
        with ThreadPoolExecutor(max_workers=4) as pool:  # independent REST calls
            list(pool.map(lambda kv: self.guard(*kv), api))
        for key, fn in [("unity_catalog", self.unity_catalog_sys), ("cluster_restart", self.cluster_restart_sys),
                        ("restricted_outbound_serverless", self.outbound_sys), ("restricted_inbound", self.inbound_sys),
                        ("restrict_workspace_admins", self.restrict_admins_sys)]:
            self.guard(key, fn, source="system_table")
        return self.rows


def collect(spark, w, catalog: str, schema: str, long_running_days: int, max_token_days: int, ctx: dict) -> dict:
    args = SimpleNamespace(long_running_days=long_running_days, max_token_days=max_token_days)
    fq = f"`{catalog}`.`{schema}`"
    rows = Collector(spark, w, args).run()
    try:
        df = spark.createDataFrame(rows, _schema())
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{fq}.rpw_security_infra_findings")
        spark.sql(f"""CREATE OR REPLACE VIEW {fq}.rpw_security_infra_latest AS
            SELECT * FROM {fq}.rpw_security_infra_findings
            QUALIFY dense_rank() OVER (ORDER BY snapshot_ts DESC) = 1""")
        spark.sql(f"""CREATE OR REPLACE VIEW {fq}.rpw_security_infra_changes AS
            WITH s AS (SELECT *, lag(status) OVER (PARTITION BY workspace_id, control_key ORDER BY snapshot_ts) AS prev_status
                       FROM {fq}.rpw_security_infra_findings)
            SELECT * FROM s WHERE prev_status IS NOT NULL AND prev_status <> status""")
    except Exception as e:  # noqa: BLE001
        raise SetupError(
            "LWGS-E301",
            what_happened=f"Writing {catalog}.{schema}.rpw_security_infra_findings failed: {type(e).__name__}: {str(e)[:300]}",
            likely_cause="The run-as identity lacks CREATE TABLE/USE SCHEMA on the folder, or serverless compute "
                         "cannot reach the catalog's storage.",
            next_steps=[f"GRANT USE CATALOG ON CATALOG {catalog} TO `RUN_AS_IDENTITY`; "
                        f"GRANT USE SCHEMA, CREATE TABLE ON SCHEMA {catalog}.{schema} TO `RUN_AS_IDENTITY`.",
                        "Re-run the job."],
            context=ctx) from e
    by_control: dict = {}
    for r in rows:
        by_control.setdefault(r[2], {}).setdefault(r[6], 0)
        by_control[r[2]][r[6]] += 1
    return {"rows": len(rows), "by_control": by_control}
