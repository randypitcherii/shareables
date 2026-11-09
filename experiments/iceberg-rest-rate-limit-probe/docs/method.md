# Method notes

The details behind the [README](../README.md): how each number was produced, plus the
dead ends that shaped the rig.

## Ramp shape and what counts as "onset"

- **Ramp:** each step runs 20 s at a fixed target rate. The open-loop scheduler fires calls
  on a timer, never waiting on responses.
- **Hold:** 45 s at 1.25× the top step.
- **Recovery:** 30 s at 2/s.
- **Skipped calls:** with all 250 in-flight slots busy, a call is logged with status `-1`
  and not sent. A slow server therefore shows up as skips, and the client never silently
  lowers the offered rate.
- **Measures** (`scripts/probe_core.py::analyze`):

| Measure | Definition |
|---|---|
| `first_429_rate` | first step with any 429 |
| `onset_rate` | first step with at least 5% 429s |
| `accepted_ceiling_rps` | median 2xx/s across the throttled steps |
| `burst_sec_at_onset` | time from the start of the onset step to its first 429. With excess *e*/s over the limit, bucket depth ≈ *e* × this. |
| `throttle_after_drop_sec` | last 429 after the drop to the trickle |
| `recovery_sec` | first clean second after the drop |
| `latency_recovery_sec` | first second with p50 latency back within 2× of the first step's p50 |

Measured burst at onset:

| Endpoint | Burst at onset | Excess over limit | Bucket depth (≈ 1 s of the limit) |
|---|---:|---:|---:|
| `loadTable` | 6-8 s | 50/s | ~300-400 |
| `config` | 3 s | 50/s | ~150 |
| `/credentials` | 3.9 s | 10/s | ~40 |

## Laptop vs in-region runs

- A `loadTable` response is ~4.1 KB (2.7 KB gzipped). One workspace at 300-400/s fits
  through a home downlink: the laptop and the in-region job measured the same 300/s
  ceiling.
- Two workspaces at 2 × 1,000/s did not fit. The laptop topped out at ~155-175 2xx/s per
  workspace with p50 ~700 ms and no 429s, which proved nothing.
- **More concurrency made it worse.** At 800 in flight, the laptop saw 429s sooner (at
  400/s) and more skips than at 250, because client-side queueing turned into server-side
  bursts.
- The scope tests therefore run as serverless jobs inside each workspace
  (`databricks/databricks.yml`). `scripts/06_run_in_workspace.py` starts one run per
  workspace and passes `--start-at <epoch>`, so both ramps start in the same second. It
  then pulls each run's Parquet log from the volume and merges it.

## Per-principal check (service principal in workspace B)

This test checks whether the shared limit is per caller identity.

1. Create a service principal in workspace B (`databricks service-principals create`).
2. Give it the `workspace-access` entitlement. Without it, Iceberg REST returns 403 "This
   API is disabled for users without … entitlements".
3. Grant it `USE CATALOG`, `USE SCHEMA`, `SELECT` and `EXTERNAL USE SCHEMA` on the probe
   schema.
4. Create an OAuth secret (`databricks service-principal-secrets-proxy create <id>`) and store
   `client_id` / `client_secret` in a secret scope in B.
5. Run `make probe-job ENDPOINT=load_table_spread WORKSPACES=a,b SP_SCOPE=b=<scope>`:
   - The job still runs as you.
   - The probe in B reads the secret and calls Iceberg REST as the service principal (the
     `sp:<scope>` profile in `scripts/_common.py`).
   - Running the whole job as the service principal (bundle `run_as`) needs the
     account-level `servicePrincipal.user` role, which is why this design avoids it.

**Result:** the combined ceiling was the same as with one user in both workspaces (~370-400/s
combined). The shared limit is not per principal.

## Attributing 429s with the audit log

`system.access.audit` with `service_name = 'uniformIcebergRestCatalog'` records
Iceberg REST calls: `config`, `loadTable`, `listTables`, `loadTableCredentials`, and so on.
It lagged by about 10 minutes.

Grouping by `workspace_id, action_name, response.status_code` over the probe window showed
the following:

- **All the per-workspace (`RESOURCE_EXHAUSTED`) 429s were missing.** Their 2xx neighbours
  were present.
- **The shared-limit (`ServiceUnavailableException`, ErrorCode 2001) 429s were present**: ~3,000
  per workspace, matching the probe's own count.
- **`listTables` showed 273 server-side 429s** that the probe never saw. Each of those calls
  had already timed out on the client (30 s) before the server rejected it.

So the audit log sees the shared limit and back-end queueing, but not the edge limit. Count
429s client-side (the probe's Parquet log or your engine's metrics) when you need the full
picture. Note that `system.access.audit` covers the whole account, so filter by
`workspace_id` when attributing.

## Legacy endpoint

`/api/2.1/unity-catalog/iceberg/v1/…` returns `400` with "Legacy Iceberg endpoints … are
deprecated. Please migrate to … /iceberg-rest/v1/". Rate limiting runs before that check
(5/s clean, 44% 429s at 8/s, 57% at 12/s), so a client stuck on the old path can fail in two
different ways.
