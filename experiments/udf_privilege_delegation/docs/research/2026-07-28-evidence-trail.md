---
title: "UDF Privilege Delegation — Evidence Trail and Measurement Traps"
tags: [unity-catalog, udf, privileges, delegation, methodology]
status: active
created: 2026-07-28
---

# Evidence trail

The README carries the conclusions. This is the raw material behind them, plus
the three places where the first version of the harness produced a *plausible
and wrong* answer. All three would have been invisible in a write-up that only
reported final statuses, which is the argument for keeping the harness itself
under review rather than only its output.

## Verbatim platform responses

Row 1, caller's direct read (the control that makes the row mean anything):

```
[INSUFFICIENT_PERMISSIONS] Insufficient privileges:
User does not have SELECT on Table
  '<catalog>.udf_privilege_delegation.sensitive_table'. SQLSTATE: 42501
```

Row 1, same caller through the admin-owned SQL UDF: `3`.

Row 2, from inside `LANGUAGE PYTHON` on a serverless warehouse, as the caller:

```
https://example.com                       -> {"reached": true, "status": 200}
<workspace>/api/2.0/preview/scim/v2/Me    -> {"reached": true, "status": 401, "http_error": true}
```

Row 3, the sandbox describing itself (identical for admin and caller):

```json
{
  "databricks_env_vars": ["DATABRICKS_ROOT_VIRTUALENV_ENV"],
  "credential_env_vars_present": [],
  "sdk_importable": true,
  "sdk_default_auth": "ValueError: default auth: cannot configure default credentials",
  "dbutils_available": false,
  "spark_session": "none"
}
```

Row 5, as a workspace admin:

```
[PARSE_SYNTAX_ERROR] Syntax error at or near 'SERVICE'. SQLSTATE: 42601 (line 1, pos 7)
CREATE SERVICE PRINCIPAL `udf-delegation-probe`
-------^^^
```

`CREATE USER` and `CREATE LOGIN` fail identically at pos 7.

Row 6, caller holding only `EXECUTE`: the sentinel planted in the function body
came back through `DESCRIBE FUNCTION EXTENDED` and through
`information_schema.routines`. `SHOW CREATE TABLE` refused with
`TABLE_OR_VIEW_NOT_FOUND` — a shape error, not an authorization one.

Row 7, caller holding only `EXECUTE`, function body carrying the author's token:
`{"outcome": "created", "status": 201}`, confirmed by reading the SCIM directory
back as the admin.

## Trap 1 — the "low-privilege" client was the admin

The first run reported that the low-privilege service principal authenticated as
`<the admin's own email>`. The credentials were correct; the SDK never used
them. The Makefile exports `DATABRICKS_CONFIG_PROFILE`, and the SDK's unified
auth resolves a config profile ahead of explicitly-passed `client_id` /
`client_secret`, so the "caller" was the author's own CLI session.

Every row would have passed. The matrix would have read: definer's rights work,
egress works, credentials are present, delegation succeeds end to end — a
completely coherent, completely false story, because the experiment would have
been the admin testing whether the admin can do things.

Fixes: pin `auth_type="oauth-m2m"`, and add `assert_distinct_identities()`, which
runs `SELECT current_user()` on both clients and aborts if they match. The
identity claim is now measured, not configured.

## Trap 2 — counting a virtualenv path as a credential

Row 3's first implementation scored "does a credential exist?" as *any*
environment variable starting with `DATABRICKS_`. The sandbox exports exactly
one — `DATABRICKS_ROOT_VIRTUALENV_ENV`, a filesystem path — so the row recorded
**pass**: "the sandbox has a credential."

The fix was to enumerate the variables that could actually authenticate a
request (`DATABRICKS_TOKEN`, `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, …) and
report that subset separately. The row flipped to **fail**. Prefix-matching a
namespace is not the same as detecting the thing the namespace sometimes
contains.

## Trap 3 — scoring a live 401 as "unreachable"

The egress probe wrapped `urlopen` in a bare `except Exception`, so the control
plane's `401` — an HTTP response, therefore proof the packet made the round trip
— was recorded as a connectivity failure. Row 2 would have read "the sandbox can
reach the public internet but not the workspace," which invites the wrong
diagnosis entirely (a firewall) instead of the right one (no credential).

`urllib.error.HTTPError` is now caught separately and recorded as
`reached: true` with the status code. Reachability and authorization are
different questions and a probe that conflates them answers neither.

## What survived

Rows 1, 5, 6, and 7 were correct on the first run and unchanged by the fixes.
Rows 2 and 3 changed status. Row 4 was correct throughout but for a reason the
first harness could not have articulated — it fails because of row 3, and rows
2 and 3 had to be measured properly before that attribution was earned.

## Environment dependence worth restating

Databricks documents serverless egress as deny-by-default under a
[network policy](https://docs.databricks.com/aws/en/security/network/serverless-network-security/network-policies).
Row 2 succeeded here, so this workspace runs a permissive policy. A restrictive
workspace would fail row 2 and reach the same overall conclusion sooner. Testing
under the *most* permissive posture available is the stronger test: the pattern
still fails without an embedded secret.
