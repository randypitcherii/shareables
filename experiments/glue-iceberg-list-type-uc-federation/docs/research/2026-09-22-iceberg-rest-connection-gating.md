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
