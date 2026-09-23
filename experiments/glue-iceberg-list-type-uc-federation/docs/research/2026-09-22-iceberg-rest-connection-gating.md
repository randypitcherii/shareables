# Why `CREATE CONNECTION … TYPE ICEBERG_REST` fails even though the type exists

_2026-09-22. Background for row 10; public-safe summary. Measured evidence is
in `results/matrix_results.json` under `10_iceberg_rest_federation`._

## The observation

Row 10 tried to federate to the AWS Glue Iceberg REST endpoint
(`https://glue.<region>.amazonaws.com/iceberg`) with three connection option
shapes — SigV4/IAM role, bearer token, and a UC service credential name. All
three were rejected before any network call with:

```
Securable kind 'CONNECTION_ICEBERG_REST_{OAUTH_M2M,BEARER_TOKEN}' is not
enabled. If this is a securable kind associated with a preview feature,
please enable it in workspace settings.
```

The connection type itself is real and present in the product — the same
workspace already holds a working `ICEBERG_REST` connection pointed at
another endpoint. Only the *auth securable kinds* needed to create a new one
are gated.

## Why nothing shows under Settings → Previews

The error text points at workspace settings, but the generic Iceberg REST
catalog connection is not a self-serve workspace preview. It is a **gated
private preview that Databricks enrolls per workspace** (per-workspace
feature flags, applied internally on request). Nothing appears in the
workspace Previews page because enrollment is not exposed to workspace
admins.

Practical consequence for a customer: hitting this error is expected, and
the fix is not in the product UI — the account team must ask Databricks to
enroll the workspace in the Iceberg REST catalog federation preview.

## How enrollment actually works, and why this row may stay ⛔ for a while

_Added 2026-09-18 after internal research._

**The Foreign Iceberg private-preview FAQ says this in so many words:** under
"What can't I do with the Foreign Iceberg Preview" — *"You cannot yet
federate to Iceberg REST Catalogs (ex: Polaris). As a workaround, you can
read these Iceberg tables using the metadata file location."* That
workaround is exactly what row 9 (shadow `register_table`) and row 8's
`CREATE TABLE … USING iceberg LOCATION '<metadata.json>'` probe measure.

**The product direction is branded connectors, not a generic IRC
connection.** The Lakehouse Federation IRC PRD scopes the next connectors to
GCP BigLake, Workday Data Cloud, and Palantir Foundry, with a **separate,
future "Glue IRC" branded connector** ("Existing HMS connector remains; new
Glue IRC connector is separate"). The *generic* `ICEBERG_REST` connection —
the thing row 10 tries to create — is listed as **"only as private
release"**, and SigV4 auth (what the Glue endpoint natively speaks) is
marked **"not prioritized"**. So even a workspace that gets the generic
type enabled may not be able to authenticate to `glue.<region>.amazonaws.com/iceberg`
without an OAuth front.

**Enrollment path, if pursued anyway:** the customer fills in the *Managed
Iceberg and Iceberg REST Catalog* onboarding survey; the preview team
enables approved applications weekly; questions go to the internal
`#iceberg-private-previews` channel. Given the FAQ wording above, expect the
answer to be "not yet" for IRC federation specifically.

**Net for the customer conversation:** row 10 is not a near-term unblock.
The realistic paths remain (a) a Databricks-side fix to federation's
schema derivation, or (b) the reader-side shadow-table pattern from row 9
with a refresh trigger.

## What is still unknown once enrolled

- **Which auth shape the Glue endpoint will accept through UC.** All three
  probes were blocked at the same gate, so the experiment has not isolated
  whether SigV4, OAuth M2M, or bearer-token auth is the supported path for
  `glue.<region>.amazonaws.com/iceberg` specifically.
- **Whether the Glue endpoint is an intended target at all**, versus
  branded connector types for specific external catalogs.

`make run-10` is written to answer both the moment the gate lifts: it
retries each auth shape, records which one creates the connection, then
builds the foreign catalog and probes the same six type shapes as rows 1–6.
