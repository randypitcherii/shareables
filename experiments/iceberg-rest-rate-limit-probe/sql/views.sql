-- Views over probe results and the audit log. {fq} = `<catalog>`.`<schema>`.
-- Created by scripts/04_upload.py after probe_calls is loaded.

CREATE OR REPLACE VIEW {fq}.v_probe_per_second AS
SELECT run_id, endpoint, workspace, phase,
       CAST(FLOOR(t_sent - MIN(t_sent) OVER (PARTITION BY run_id, endpoint)) AS INT) AS t,
       target_rate, status, latency_ms
FROM {fq}.probe_calls;

CREATE OR REPLACE VIEW {fq}.v_probe_seconds AS
SELECT run_id, endpoint, workspace, t,
       MAX(target_rate) AS target_rate,
       COUNT_IF(status <> -1) AS attempted,
       COUNT_IF(status = -1) AS skipped,
       COUNT_IF(status BETWEEN 200 AND 299) AS succeeded,
       COUNT_IF(status = 429) AS throttled,
       COUNT_IF(status NOT BETWEEN 200 AND 299 AND status NOT IN (429, -1)) AS other,
       PERCENTILE(latency_ms, 0.5) FILTER (WHERE status BETWEEN 200 AND 299) AS p50_ms,
       PERCENTILE(latency_ms, 0.95) FILTER (WHERE status BETWEEN 200 AND 299) AS p95_ms
FROM {fq}.v_probe_per_second
GROUP BY ALL;

CREATE OR REPLACE VIEW {fq}.v_probe_seconds_long AS
SELECT run_id, endpoint, workspace, t, series, value
FROM {fq}.v_probe_seconds
LATERAL VIEW STACK(4,
  'attempted', CAST(attempted AS DOUBLE), 'succeeded (2xx)', CAST(succeeded AS DOUBLE),
  '429', CAST(throttled AS DOUBLE), 'target rate', CAST(target_rate AS DOUBLE)
) s AS series, value;

CREATE OR REPLACE VIEW {fq}.v_probe_steps AS
SELECT run_id, endpoint, workspace, target_rate,
       COUNT(*) AS calls,
       ROUND(COUNT_IF(status = 429) / COUNT(*), 4) AS throttled_share,
       PERCENTILE(latency_ms, 0.5) FILTER (WHERE status BETWEEN 200 AND 299) AS p50_ms
FROM {fq}.probe_calls
WHERE phase = 'ramp' AND status <> -1
GROUP BY ALL;

CREATE OR REPLACE VIEW {fq}.v_probe_onset AS
SELECT run_id, endpoint, workspace,
       MIN(target_rate) FILTER (WHERE throttled_share > 0) AS first_429_rate,
       MIN(target_rate) FILTER (WHERE throttled_share >= 0.05) AS onset_rate,
       MAX(target_rate) AS peak_target_rate
FROM {fq}.v_probe_steps
GROUP BY ALL;

-- Iceberg REST activity from the audit log (lags ~10-15 min). The IRC service logs as
-- uniformIcebergRestCatalog; the UC calls it makes underneath log as unityCatalog.
-- Requests rejected at the API proxy edge never reach IRC, so they may be missing here.
CREATE OR REPLACE VIEW {fq}.v_iceberg_rest_audit AS
WITH per_second AS (
  SELECT DATE_TRUNC('SECOND', event_time) AS second,
         workspace_id, service_name, action_name,
         response.status_code AS status_code,
         COUNT(*) AS calls
  FROM system.access.audit
  WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
    AND (service_name = 'uniformIcebergRestCatalog'
         OR (service_name = 'unityCatalog'
             AND action_name IN ('getTable', 'generateTemporaryTableCredential', 'listTables')))
  GROUP BY ALL
)
SELECT DATE_TRUNC('MINUTE', second) AS minute,
       workspace_id, service_name, action_name, status_code,
       SUM(calls) AS calls,
       MAX(calls) AS peak_qps
FROM per_second
GROUP BY ALL;

CREATE OR REPLACE VIEW {fq}.v_iceberg_rest_audit_failures AS
SELECT event_time, workspace_id, service_name, action_name,
       response.status_code AS status_code,
       LEFT(response.error_message, 300) AS error_message,
       user_agent
FROM system.access.audit
WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND service_name = 'uniformIcebergRestCatalog'
  AND response.status_code >= 400;
