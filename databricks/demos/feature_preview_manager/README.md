# Feature Preview Manager

A Databricks notebook that lists every feature preview in a workspace, or in an
account and all of its workspaces. For each one it shows whether the preview is
on, whether someone set it explicitly, and whether it is off only because of its
default. An optional cell then turns those default-off previews on in bulk, with
safety checks.

It uses only the public
[Settings v2 API](https://docs.databricks.com/aws/en/admin/workspace-settings/settings-api-manage).

## When this is useful

- A workspace's preview defaults changed (for example, after a pricing-tier or
  compliance-profile change) and previews that were on by default are now off.
- You want an audit of preview state across many workspaces.
- You want to roll a set of previews out to every workspace in one pass.

## Files

| File | Purpose |
| --- | --- |
| `feature_preview_manager.py` | The notebook (Databricks source format). Import it into a workspace. |
| `test_offline.py` | Offline test with fake clients: `uv run test_offline.py`. |

## How state is read

For each preview the notebook calls `GET /api/2.1/settings/{name}`. The response
has two values:

| Field | Meaning |
| --- | --- |
| `effective_boolean_val` | What is in effect now. This is what the Previews page shows. |
| `boolean_val` | Present only if someone set the preview explicitly, in the UI or through the API. |

The inventory derives these columns from that response:

| Column | Meaning |
| --- | --- |
| `effective_enabled` | Whether the preview is on now. |
| `manually_set` / `manual_value` | Whether someone set it explicitly, and to what. |
| `on_default` | Nobody set it. It follows the default. |
| `off_by_default` | `on_default` and currently off. These are the candidates to enable. |

## Usage

1. Import `feature_preview_manager.py` into a workspace
   (**Workspace → Import → File**).
2. Attach the notebook to **serverless** compute.
3. Set the widgets:

   | Widget | Notes |
   | --- | --- |
   | `scope` | `current_workspace` needs no setup. `account` covers the account and every running workspace in it. |
   | `account_id`, `accounts_host` | Account scope only. The host defaults to the AWS accounts console. Change it for Azure or GCP. |
   | `secret_scope`, `client_id_key`, `client_secret_key` | Account scope only. These point to the credentials of an OAuth machine-to-machine (M2M) service principal. |
   | `workspace_filter` | Comma-separated workspace IDs or names. Blank means all. |
   | `max_workers` | Number of API calls run in parallel. Lower it if you hit rate limits. |
   | `output_table` | Optional `catalog.schema.table` prefix. The notebook appends rows to `<prefix>_inventory` and `<prefix>_enable_attempts`. |

4. Run sections 1–2. You get a full inventory, a summary per preview, and an
   errors table. This part only reads.
5. *(Optional)* Section 3 enables previews. It is a **dry run** unless
   `dry_run = false` and `confirm = ENABLE`. Narrow what it changes with the
   phases, only-these, and never-these widgets. Account-level previews are
   left alone unless you include them.

To retry failures, run sections 2–3 again. Anything already enabled now counts
as explicitly set and drops out of the plan.

### Enablement statuses

| Status | Meaning |
| --- | --- |
| `ENABLED` | The PATCH succeeded and reading the setting back shows it on. |
| `SET_BUT_NOT_EFFECTIVE` | The value saved, but the preview is still not in effect, for example because of entitlements or a compliance profile. |
| `SKIPPED_NOW_MANUALLY_SET` | Someone set the preview between the inventory and the write. It was left alone. |
| `FAILED` | The error is recorded. The run continues. |
| `DRY_RUN` | Nothing was written. |

## Permissions

- **Current workspace:** the user running the notebook must be a workspace
  admin.
- **Account scope:** you need an OAuth M2M service principal that is an
  **account admin** *and* a **workspace admin in every workspace** you want to
  cover. Store its client ID and secret in a secret scope. Workspaces where it
  isn't an admin show up as rows in the errors table and don't stop the run.

## Caveats

- **Enabling a preview pins it.** The notebook writes an explicit value. From
  then on the preview no longer follows its default. If the default later
  changes, for example because the preview's rollout policy or your
  workspace's tier changes, the pinned value stays.
- **The API cannot return a preview to its default.** Changing a preview again
  writes another explicit value. Keep the `_enable_attempts` table, or the
  results table, as your record of what changed.
- **Defaults depend on context.** They vary with the preview's release stage,
  your pricing tier, any compliance or security profile, cloud, and region.
  If previews went off because of a tier or profile change, reversing that
  change may restore the defaults by itself. In that case the inventory alone
  may be all you need.
- **Previews are pre-release features.** Beta and Private Preview features can
  change behaviour, have limited support, or require particular runtimes or
  regions. Review the dry-run list, and the docs linked in the inventory,
  before applying.
- **Saving a value doesn't guarantee it takes effect.** Entitlements or
  compliance controls can keep a preview off. The notebook reports this as
  `SET_BUT_NOT_EFFECTIVE`.
- **The preview list changes over time.** Previews are added, renamed, and
  retired. The notebook always lists them through `settings-metadata` and
  never hard-codes names.
- **Every change is audited.** Writes run as the identity running the notebook
  and appear in that workspace's audit logs.
- **Rate limits apply.** The notebook retries throttling and temporary errors
  with backoff. For very large accounts, lower `max_workers`.
- **This is not official tooling.** Test it on a non-production workspace
  first. See the repository disclaimer.
