# Lakewatch getting started

A Databricks Asset Bundle that turns on **Lakewatch (Beta)** in your workspace
only when that is safe. It then deploys a starter set of Databricks security infrastructure
detections. One command deploys it and one job sets it up. Every object it
creates has a unique `rpw_` internal name (catalog, schema, tables, bundle resource keys), so nothing you already have is overwritten. Display names are plain English.

> Lakewatch is in Beta. The default (`prod`) deploy turns the detection rules
> **on**, so they run on their schedules and raise detections right away. Review
> them with your security team. To register them switched off, deploy with
> `--var rules_enabled=false`. The `dev` target keeps them off.

## What it does

The job **Lakewatch Getting Started** (bundle key `rpw_lakewatch_getting_started`) runs as one serverless task. Every step
is idempotent, so re-running it is always safe.

| Step | What happens |
| --- | --- |
| 1. Preflight | Checks that the job runs as a workspace admin and that Unity Catalog is enabled. |
| 2. Observe | Reads the Lakewatch preview toggle, any existing Lakewatch environments, and any earlier preview setup. |
| 3. Decide | Picks one of the paths below. A stop path changes nothing and explains why. |
| 4. Environment | Only if Lakewatch is not set up yet: creates the catalog on your metastore's default storage, then turns the Beta on and registers and initializes the catalog. On a workspace that has never used Lakewatch, registration can take a few minutes after the Beta is turned on; the job waits and retries. |
| 5. Folder | Creates the Lakewatch folder (schema) `rpw_databricks_security_infra`. |
| 6. Content | Writes a Databricks security infrastructure snapshot. At the same time it registers the audit-log rules, then registers the rules that read the snapshot. |

### How it decides

| Your workspace today | What the job does |
| --- | --- |
| Lakewatch has never been set up | Turns on the Lakewatch Beta preview, creates catalog `rpw_lakewatch`, and sets it up as the Lakewatch environment. |
| Lakewatch Beta is already set up on the catalog you deploy with | Reuses it and changes no Lakewatch settings. |
| Lakewatch Beta is set up on another catalog | Stops (`LWGS-E104`) and prints the exact redeploy command. |
| An earlier Lakewatch preview is set up, but the Beta is not | Stops (`LWGS-E101`). Moving from the preview to the Beta is your decision. The job never modifies preview resources. To set up the Beta alongside the preview, opt in with `--var allow_with_prior_preview=true`. |

## What gets deployed

| Object | Name |
| --- | --- |
| Job (serverless, performance optimized) | **Lakewatch Getting Started** (daily 06:15 UTC; paused in the `dev` target) |
| SQL warehouse (serverless, 2X-Small, auto-stop 5 min) | **Lakewatch Getting Started Warehouse** (runs the rules and the dashboard) |
| Dashboard | **Databricks Security Infrastructure** |
| Catalog (only if Lakewatch is not set up yet) | `rpw_lakewatch` |
| Lakewatch folder (schema) | `<catalog>.rpw_databricks_security_infra` |
| Tables and views | `rpw_security_infra_findings`, `rpw_security_infra_latest`, `rpw_security_infra_changes` |
| Lakewatch rules (on in `prod`, off in `dev`) | the 7 `databricks_security_infra_*` rules listed below |

### Security infrastructure checks (`rpw_security_infra_findings`)

| Control | Evidence |
| --- | --- |
| Restricted inbound (IP access lists or Private Link) | API for this workspace. IP access denials in audit logs for other workspaces. |
| Restricted outbound for serverless | Serverless network-policy events |
| No public IP (Azure) | Reported as needing a manual check |
| Unity Catalog adoption | Share of the last 30 days of compute on Unity Catalog access modes |
| Restrict workspace admins | API for this workspace. Settings changes in audit logs for others. |
| Cluster restart (patch hygiene) | Clusters up longer than `long_running_days` |
| PAT maximum lifetime, long-lived tokens, service principals holding admin, verbose audit logs | API |

The API checks cover the workspace the job runs in. The system-table checks
cover every workspace in the account that the run-as identity can see in
`system.*`.

### Detection rules (`rules/*.yml`, same YAML as Lakewatch's "Edit as YAML")

| Rule | Detects |
| --- | --- |
| `databricks_security_infra_credential_use_from_denied_ip` | A credential is used from an IP address that your IP access lists rejected. |
| `databricks_security_infra_service_principal_new_source_ip` | A service principal authenticates from an IP address not seen in its baseline. |
| `databricks_security_infra_ip_access_list_weakened` | An IP access list is deleted, disabled, or widened. |
| `databricks_security_infra_restrict_workspace_admins_disabled` | The restrict-workspace-admins setting is relaxed. |
| `databricks_security_infra_pat_created_long_lived` | A personal access token is created with no expiry or a lifetime over 90 days. |
| `databricks_security_infra_control_regressed` | An essential control that passed is now failing. |
| `databricks_security_infra_control_failing` | An essential control is still failing (a weekly reminder). |

## Prerequisites

- **Workspace admin.** The job runs as the person who deploys it.
- **Unity Catalog** enabled for the workspace.
- **Serverless compute** for jobs and SQL. It is on by default for accounts created after 2024-12-16.
- **If Lakewatch is not set up yet,** `CREATE CATALOG` on the metastore and a metastore storage root. Without one, set `catalog_storage_root` (see `LWGS-E201`).
- **For the account-wide checks,** read access to `system.access`, `system.billing` and `system.compute`.
- **A recent Databricks CLI** (tested with v1.26), authenticated to the workspace.

## Deploy and run

```bash
databricks bundle deploy                                   # prod target (the default)
databricks bundle run rpw_lakewatch_getting_started --params dry_run=true   # optional: print the plan only
databricks bundle run rpw_lakewatch_getting_started
```

If Lakewatch is already set up, find its catalog with
`databricks api get /api/2.0/lakewatch/environments`, then deploy with it. For example, for catalog `security`:

```bash
databricks bundle deploy --var catalog=security
databricks bundle run rpw_lakewatch_getting_started
```

When the run finishes, open Lakewatch from the app switcher (the grid icon at
the top left) and look in the `rpw_databricks_security_infra` folder. The rules
are already running there. Tune or switch off any rule in Lakewatch, or redeploy
with `--var rules_enabled=false` to switch them all off. The dashboard
refreshes daily, because the job's schedule is on.

### Prod or dev?

**Use the default, `prod`, for your first run and for running this as-is.** It
deploys one copy per workspace with plain names and turns on the daily
schedule. Prod is the right choice when you are using the bundle as
downloaded, not maintaining your own version.

**Switch to `dev` once you keep this as an ongoing solution in your own git
repository**, and several people change it. Each person then gets a private
copy for testing (`databricks bundle deploy -t dev`). In that copy every name
is prefixed with `[dev your_name]` and the schedule is paused. Changes reach
the shared copy only through `databricks bundle deploy -t prod`, ideally from
CI. Both targets use the same catalog and folder unless you set
`--var schema=...` for dev.

### Variables

Variables are set at deploy time (`databricks bundle deploy --var name=value`), then picked up by the next `bundle run`. `dry_run` can also be set per run.

| Variable | Default | Purpose |
| --- | --- | --- |
| `catalog` | `rpw_lakewatch` | The Lakewatch environment catalog. It is created only if Lakewatch is not set up. |
| `schema` | `rpw_databricks_security_infra` | The Lakewatch folder for this bundle's tables and rules. |
| `rules_enabled` | `true` in `prod`, `false` in `dev` | Schedule the rules when they are registered. |
| `allow_with_prior_preview` | `false` | Set up the Beta next to an earlier preview setup. |
| `catalog_storage_root` | empty | Only if the job creates the catalog and your metastore has no storage root: a storage URL under an external location you can use. |
| `dry_run` | `false` | Print the plan and change nothing. Also a job parameter, so you can set it per run: `bundle run ... --params dry_run=true`. |
| `error_beacon` | `true` | Report only the outcome code. See [Telemetry](#telemetry). |
| `long_running_days` / `max_token_days` | `30` / `90` | Check thresholds (cluster uptime, token lifetime). |

## When something goes wrong

Every known failure stops with a code, for example `LWGS-E201`. The task
output contains a block between `COPY FROM HERE` and `COPY TO HERE`. Paste that
whole block into your AI assistant: Genie Code in the workspace, Claude,
ChatGPT, Codex or another tool. The block explains what happened, the likely
cause, the next steps and the relevant environment facts, so the assistant can
walk you through the fix. `bundle run` prints the block in your terminal, and
the failed run's page in the Databricks UI shows it too. Fix the issue, then
re-run the job.

**Brand-new workspace, `Input/output error` reading `/Workspace/...`:** in a
workspace's first minutes, serverless compute can fail to read deployed files
(`CustomerEncryptionKeyIsIncorrect`, or a 403 about encryption keys). This has
lasted 15 minutes or more on new workspaces. Wait, then re-run. If it still
fails, re-upload the files. Deploy only uploads what changed locally, so clear
the local sync record too:

```bash
databricks workspace delete /Workspace/Users/YOUR_USER/.bundle/lakewatch-getting-started/prod/files --recursive
rm -rf .databricks/bundle/prod/sync-snapshots
databricks bundle deploy
```

| Code | Meaning |
| --- | --- |
| `LWGS-E001` | The job is not running as a workspace admin. |
| `LWGS-E002` | Unity Catalog is not enabled. |
| `LWGS-E101` | An earlier preview is set up and the Beta is not. The job stopped on purpose. |
| `LWGS-E102` | The Beta preview could not be turned on. It may not be offered for this cloud, region or tier. |
| `LWGS-E103` | The Beta is on, but the Lakewatch service is not reachable yet. |
| `LWGS-E104` | Lakewatch is set up on a different catalog. The block prints the redeploy command for each existing environment. |
| `LWGS-E201` | Your metastore has no storage root, so a new catalog needs a location. The block lists the external locations it can see and the exact redeploy command (`--var catalog_storage_root=...`). Nothing has been turned on yet. |
| `LWGS-E202` | The catalog could not be created. This is usually a missing `CREATE CATALOG` permission. |
| `LWGS-E203` | The catalog couldn't be registered as the Lakewatch environment. On a new Lakewatch workspace, wait 10 minutes and re-run. |
| `LWGS-E204` / `E205` | Initialization or folder creation failed. |
| `LWGS-E301` | The security infrastructure tables could not be written. |
| `LWGS-E401` | Some rules failed to register. Rules that succeeded are kept. |
| `LWGS-E999` | An unexpected error. The block includes the failing step. |

## Telemetry

API calls from the job identify themselves in the HTTP user agent as
`rpw-lakewatch-getting-started/<version>`. With `error_beacon=true`, the
default, the job makes one more call at the end of each run. That call's user
agent carries only the outcome code, `ok` or `LWGS-E###`. This lets the
bundle's maintainers see which setup errors are common and fix them. No error
text, names or data are sent. To turn it off, deploy with
`--var error_beacon=false`.

## Removing it

`databricks bundle destroy` (add `-t dev` for a dev copy) removes the job, the warehouse and the dashboard.
Lakewatch content stays, so you don't lose detection history. To remove it,
delete the `databricks_security_infra_*` rules and the `rpw_databricks_security_infra` folder in Lakewatch. If
this bundle created the `rpw_lakewatch` catalog, you can drop that too.

## Development

```bash
uv run --group dev pytest        # offline tests: state machine, error rendering, rule lint, API mapping
databricks bundle validate
```

The setup logic lives in `src/rpw_lakewatch/`:

| File | Role |
| --- | --- |
| `state.py` | The pure decision policy |
| `platform.py` | API calls and error mapping |
| `rules.py` | Rule upsert |
| `security_infra.py` | Databricks security infrastructure checks |

Lakewatch rules and environments are not yet bundle resource types. When they
are, `rules/*.yml` and the environment step should move into `resources/`,
and the matching Python can be deleted.
