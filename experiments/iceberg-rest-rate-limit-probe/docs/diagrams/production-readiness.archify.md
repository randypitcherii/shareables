# Can Snowflake read Unity Catalog Iceberg tables at production scale? {#snowflake-uc-scale}

**Yes, with a staged rollout.** The design holds up. Before you scale out, check three things: the metadata call rate, the Private Link sign-in path, and a gap in monitoring.

- At 30-second refresh, Snowflake's `loadTable` rate exceeds the measured per-workspace limit once the estate passes about 9,000 tables.[](source:probe)
- On Azure, Snowflake can't use Entra sign-in for catalog integrations over Private Link.[](source:sf-rest)
- Per-workspace 429s never reach `system.access.audit`, so audit dashboards miss them.[](source:probe)
- UniForm and Iceberg v3 are GA. The open issues are tuning and monitoring, not missing features.[](source:iceberg-v3)

@answer scope="Measured on 2 fresh sandbox workspaces sharing 1 metastore, Nov 2026. Limits are server configuration and can change; treat them as a snapshot, not an SLA."
@meta locale=en
@source probe title="Iceberg REST rate-limit probe: method and results" url=https://github.com/randypitcherii/shareables/tree/main/experiments/iceberg-rest-rate-limit-probe
@source sf-jit title="Snowflake: just-in-time refresh for external catalog metadata (private preview)" url=https://docs.snowflake.com/en/LIMITEDACCESS/iceberg/just-in-time-refresh
@source sf-rest title="Snowflake: catalog integration for Iceberg REST" url=https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest
@source sf-cldb title="Snowflake: catalog-linked databases" url=https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database
@source iceberg-v3 title="Databricks: Iceberg v3 features" url=https://docs.databricks.com/aws/en/iceberg/iceberg-v3
@source uniform title="Databricks: read Delta tables with Iceberg clients" url=https://docs.databricks.com/aws/en/delta/uniform
@source audit title="Databricks: audit log system table" url=https://docs.databricks.com/aws/en/admin/system-tables/audit-logs

## How many loadTable calls will Snowflake make? {#sizing}

Each refresh tick calls `loadTable` once per active table, so calls per second ≈ active tables ÷ refresh interval in seconds. Example: 10,000 tables ÷ 30 s = 333/s, which is over the 300/s clean ceiling.

```mermaid title="Estimated loadTable calls per second by table count and refresh interval (rule of thumb; ceiling measured in sandbox)"
xychart-beta
  x-axis ["1,000 tables", "5,000 tables", "10,000 tables", "30,000 tables"]
  y-axis "loadTable per second" 0 --> 1000
  bar "30 s refresh" [33, 167, 333, 1000]
  bar "120 s refresh" [8, 42, 83, 250]
  bar "300 s refresh" [3, 17, 33, 100]
  line "Per-workspace clean ceiling (300/s)" [300, 300, 300, 300]
```

@callout level=warning title="Snowflake JIT refresh: size for peak, not average"
Snowflake's Just-in-Time refresh (private preview) refreshes metadata at query time, on top of polling.[](source:sf-jit) With the default 300 s cache window and 30 s polling, reads rarely add calls, so the formula above still holds on average. Commits and refreshes bunch up, so compare your peak rate, not the average, with the ceiling.
A cache window shorter than the poll interval adds about one call per table per query. Writes always refresh. A failed refresh can block the query.[](source:sf-jit)

## What did the probe measure? {#measured}

| Endpoint | Clean up to | 429s start at | Notes |
| --- | --- | --- | --- |
| `loadTable` | 300/s | 350/s {caution:Binds first} | The circulating figure is 400–500/s |
| `config` | 150/s | 200/s | Client setup call |
| `/credentials` | 40/s | 50/s | Only when using vended credentials |
| `listTables` | about 50/s | never | Slows from 117 ms to 8 s instead {caution:Silent} |
| Legacy `/iceberg` | 5/s | 8/s {bad:Avoid} | Use `/api/2.1/unity-catalog/iceberg-rest` |
| Two workspaces, same metastore | about 400/s combined | above 400/s | Shared limit; adding workspaces doesn't add capacity {caution:Shared} |

Burst allowance is about 1 second. Throughput recovers within 0 to 1 seconds once load drops.[](source:probe)

## What happens when Snowflake hits a limit? {#throttle}

@sequence id=throttle-flow title="Two kinds of 429"
| actor sf | Snowflake | external |
| actor irc | Iceberg REST edge | security |
| actor ucm | Shared UC budget | security |
| actor audit | Audit log | database |
| phase | Per-workspace limit |
| sf | irc | loadTable burst | emphasis |
| irc | sf | 429 Retry-After 1 | return |
| sf | irc | retry after 1 s | |
| irc | sf | 200 | return |
| phase | Shared limit |
| sf | irc | loadTable | emphasis |
| irc | ucm | UC getTable | |
| ucm | irc | over budget | return |
| irc | audit | log 429 | dashed |
| irc | sf | 429 no Retry-After | return |

@steps id=throttle-steps
| edge | Iceberg REST edge | Rejects traffic over the per-workspace limit and sends `Retry-After: 1`. These rejections are not audited. | throttle-flow | irc |
| budget | Shared UC budget | Rejects traffic over the shared limit with a `ServiceUnavailableException`. Latency rises first. | throttle-flow | ucm |
| log | Audit log | Records only the shared-limit 429s, about 10 to 15 minutes late. | throttle-flow | audit |

## Which risks need a mitigation before go-live? {#risks}

| Risk | Impact | Mitigation |
| --- | --- | --- |
| JIT refresh fails on a 429 | Query is blocked, not served stale {bad:High} | Confirm JIT status and cache setting with Snowflake |
| 30 s refresh on a large estate | 429s and stale tables {bad:High} | Set `REFRESH_INTERVAL_SECONDS` from table count |
| Azure Private Link with Entra | Integration fails to sign in {bad:Blocker if used} | Confirm the network path; use a service principal secret if needed |
| Per-workspace 429s not audited | Throttling goes unseen {caution:Medium} | Also watch `SYSTEM$AUTO_REFRESH_STATUS` in Snowflake |
| Shared-limit 429s without Retry-After | Client retries too fast {caution:Medium} | Confirm Snowflake backs off |
| Async Iceberg metadata | Snowflake lags Delta {caution:Low} | Monitor `converted_delta_version` |
| Conversion failures on some tables | Tables missing in Snowflake {caution:Medium} | Apply the UniForm fixes per error |

UniForm fixes cover deletion vectors, column mapping, unsupported types and `REORG ... UPGRADE UNIFORM`.[](source:uniform) Audit queries filter on `service_name = 'uniformIcebergRestCatalog'`.[](source:audit)

## How should the rollout go? {#rollout}

@workflow id=rollout-flow title="Staged rollout"
| prep | Size and wire up | backend |
| pilot | Pilot tables | external |
| ok | Clean for a week? | backend |
| grow | Few thousand tables | external |
| full | Full estate | external |
| prep > pilot | |
| pilot > ok | |
| ok > grow | yes | emphasis |
| ok > prep | no | dashed |
| grow > full | | emphasis |

@steps id=rollout-steps
| s-prep | Size and wire up | Set the refresh interval from table count, confirm the Private Link sign-in path, and start the audit and Snowflake refresh monitors. | rollout-flow | prep |
| s-pilot | Pilot tables | Start with representative tables from several schemas, including the largest. | rollout-flow | pilot |
| s-ok | Clean for a week? | Look for 429s in either monitor and for conversion failures. If you see any, go back and resize. | rollout-flow | ok |
| s-grow | Few thousand tables | Re-check the peak call rate against about 300/s per workspace and about 400/s for the metastore. | rollout-flow | grow |

## What should you confirm before go-live? {#checklist}

- [ ] Count the tables Snowflake will read and the freshness each consumer needs
- [ ] Check whether the Snowflake account has JIT refresh, and which `ICEBERG_METADATA_CACHE_SECONDS` applies
- [ ] Confirm the network path (Private Link or public) and the sign-in method (Entra OAuth or service principal secret)
- [ ] Check every table for UniForm conversion errors before Snowflake discovers it
- [ ] Alert on 429s in both `system.access.audit` and Snowflake's refresh status
