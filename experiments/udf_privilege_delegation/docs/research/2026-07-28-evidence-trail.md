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

## Trap 4 — an authorization refusal wearing an HTTP status code

Rows 8–11 and 14 were added on 2026-07-29 and immediately reproduced the same
family of mistake, from the other direction.

`http_request()` does not fail the statement when Unity Catalog refuses the
connection. It returns, successfully, a `STRUCT<status_code, text>` whose
`status_code` is `403` and whose `text` carries the refusal:

```
{"status_code":"403","text":"[REMOTE_FUNCTION_HTTP_FAILED_ERROR] The remote HTTP
request failed with code 403, and error message 'HTTP request failed with status:
{"error_code":"PERMISSION_DENIED","message":"Failed request to <host>. Error:
User is missing USE CONNECTION on <connection>"}' SQLSTATE: 57012"}
```

The first version of row 8 scored on `outcome.succeeded`, so the caller's refusal
counted as a working call and the row recorded **inconclusive** on the grounds
that the caller "could use the connection directly". Read one step further and it
would have recorded **pass** — definer's rights work — which is the opposite of
what happened.

This is trap 3 inverted. There the harness treated an HTTP answer as a
connectivity failure; here it treated an authorization failure as an HTTP answer.
The general form is the same: `http_request()` returns a transport-shaped value
for both transport outcomes and authorization outcomes, so the status code alone
never says which one occurred. Scoring now goes through
`_delegation_common.http_request_outcome`, which classifies on the body, and a
call counts as allowed only when the platform did not refuse it *and* the origin
answered.

## Trap 5 — one probe cannot separate "unreachable" from "not allowed"

Row 11 asks whether `http_request()` can reach this workspace's own SCIM API. The
first version asked it once, with the sentinel credential, and got:

```
{"status_code":"401","text":"... {"error_code":401,"message":"Credential was not
sent or was of an unsupported type for this API. [ReqId: ...]"}"}
```

Scored as "refused", which was true and useless. A 401 about the credential is
evidence the request *arrived* — DNS, egress and TLS all worked and the platform
got as far as looking at what was presented. Reporting that as unreachable
invites the firewall diagnosis all over again.

The same call with a valid credential is refused `403`, by the workspace's IP
access list, naming the egress address. That is the real answer, and it can only
appear after authentication succeeds — an access list has nothing to evaluate
until it knows who is asking.

So the row runs both probes deliberately: the sentinel establishes that the
round trip completes, and the live credential establishes that the authenticated
request is then refused at the perimeter. Either probe alone tells a coherent
and wrong story.

## What survived

Rows 1, 5, 6, and 7 were correct on the first run and unchanged by the fixes.
Rows 2 and 3 changed status. Row 4 was correct throughout but for a reason the
first harness could not have articulated — it fails because of row 3, and rows
2 and 3 had to be measured properly before that attribution was earned.

Of the 2026-07-29 additions, rows 9, 10, 12 and 13 were correct on the first run.
Row 8 changed status once its scoring was fixed (trap 4) and row 11 changed once
it stopped asking its question with a single probe (trap 5). Row 14 was written
after the fixes and inherits both.

## Verbatim platform responses, 2026-07-29

Row 8, the caller through the admin-owned SQL UDF, holding only `EXECUTE`:

```
PERMISSION_DENIED: Failed request to <host>. Error:
User is missing USE CONNECTION on <connection>
```

Identical, string for string, to what the same caller got calling
`http_request()` directly. That identity is the finding.

Row 9, the same caller *after* `GRANT USE CONNECTION`, running
`DESCRIBE CONNECTION`:

```
Connection Name  <connection>
Type             HTTP
Owner            author@example.com
Read-only        true
Options          auth_scheme -> bearer, host -> <host>, port -> 443, base_path -> /
```

No `bearer_token`. `SHOW CONNECTIONS`, `system.information_schema.connections`
and the connections REST API were all allowed and all silent about it too.

Row 10, the same caller, request shapes the author's function never makes:

```
GET  <base_path>                    -> 200   (origin answered)
GET  some/other/resource            -> 404   (origin answered)
POST <base_path>                    -> 405   (origin answered)
DELETE <base_path>                  -> 405   (origin answered)
GET  ../../etc/passwd               -> [INVALID_HTTP_REQUEST_PATH] rejected
```

A 404 is a stronger result than a 200 here: it can only have come from the far
end, so the platform dispatched a call nobody authored.

Row 12, the caller triggering the broker job with the two suffixes:

```json
{"requested_suffix": "alpha", "run_as": "author@example.com", "outcome": "created",
 "created_display_name": "udf-delegation-job-sp-alpha"}
{"requested_suffix": "../Evil Name", "outcome": "rejected",
 "reason": "suffix must match ^[a-z0-9-]{1,24}$"}
```

Row 13, the same caller against the job's edges: `edit_task`, `change_run_as` and
`regrant_self` all `PermissionDenied`; reading the notebook the job runs came
back `ResourceDoesNotExist`, which is a refusal that hides the object's existence
rather than naming the privilege — still a refusal, but not an authorization
answer, and the README says so rather than rounding it up.

Row 14, the caller through a `SQL SECURITY DEFINER` procedure, with
`USE CONNECTION` revoked:

```
401 Credential was not sent or was of an unsupported type for this API. [ReqId: ...]
```

Not the `USE CONNECTION` refusal the UDF produced, and not a working call. The
`ReqId` marks it as platform-emitted rather than anything the target host would
say.

## Environment dependence worth restating

Databricks documents serverless egress as deny-by-default under a
[network policy](https://docs.databricks.com/aws/en/security/network/serverless-network-security/network-policies).
Row 2 succeeded here, so this workspace runs a permissive policy. A restrictive
workspace would fail row 2 and reach the same overall conclusion sooner. Testing
under the *most* permissive posture available is the stronger test: the pattern
still fails without an embedded secret.
