# scripts/

Every ✅/❌ in the README matrix is produced here. One numbered script per matrix
row; the number ties script → row → entry in `results/matrix_results.json`.

## Prerequisites

- A Databricks CLI profile for a **workspace admin** — the setup scripts create
  service principals and mint their OAuth secrets. SSO/OAuth only; `_common.py`
  exits if `DATABRICKS_TOKEN` looks like a `dapi…` personal access token.
- A **serverless** SQL warehouse. UC Python UDFs do not run on classic warehouses.
- A Unity Catalog catalog the profile can create schemas in.
- **`CREATE CONNECTION` on the metastore** for rows 8–11 and 14.
- Permission to create jobs and upload a notebook for rows 12–13.

## Shared modules

| File | Purpose |
|---|---|
| `_common.py` | Auth (`admin_client`, `lowpriv_client`), `.env` loading, SQL execution, results writing, redaction, object names shared across rows. |
| `delegation/_delegation_common.py` | Function invocation, JSON decoding, and `http_request_outcome` — the classifier rows 8, 10, 11 and 14 score on. |

Three things in `_common.py` are load-bearing and easy to break:

- **`lowpriv_client()` pins `auth_type="oauth-m2m"`.** The Makefile exports
  `DATABRICKS_CONFIG_PROFILE`, and without the pin the SDK's unified auth
  resolves the CLI profile first and silently returns the *admin* identity —
  turning every row into a self-test that always passes.
- **`assert_distinct_identities()` is called by every row that uses both
  identities.** It runs `SELECT current_user()` on each client and refuses to
  continue if they match. Configuration is not trusted; the warehouse is asked.
- **`write_result()` redacts.** Catalog name, workspace host, caller application
  id, anything email-shaped, any IPv4 address, and any credential a row passes to
  `register_secret()` are replaced with placeholders before anything is written,
  because raw platform error strings are exactly where real coordinates hide and
  `results/` is committed to a public repo. Only row 11 registers a credential,
  and only because its question cannot be answered without presenting one.

`http_request_outcome` in `delegation/_delegation_common.py` is load-bearing for
the same reason: `http_request()` returns a `STRUCT<status_code, text>` for both
transport outcomes *and* authorization refusals, so a Unity Catalog
`PERMISSION_DENIED` arrives as `status_code = 403` inside a statement that
succeeded. Scoring on `outcome.succeeded` reads a refusal as a working call. The
classifier keys on the body and counts a call as allowed only when the platform
did not refuse it and the origin answered — see trap 4 in the evidence trail.

## Setup and teardown

| Script | What it does |
|---|---|
| `verify.py` | One real request (`SELECT current_user()`, then `SHOW SCHEMAS`) to prove profile + warehouse + catalog before any matrix work. |
| `setup/00_create_lowpriv_principal.py` | Creates the caller service principal, mints a workspace OAuth secret, grants `CAN_USE` on the warehouse, asserts it is not in `admins`, writes the gitignored `.env`, and proves it authenticates as itself. |
| `setup/01_provision_objects.py` | Creates the schema, `sensitive_table`, the four probe functions, and grants the caller `USE CATALOG` / `USE SCHEMA` / `EXECUTE` — then asserts via `SHOW GRANTS` that the caller holds *nothing* on `sensitive_table`. |
| `setup/02_provision_connection.py` | Creates the HTTP connection carrying a sentinel token, the SQL UDF and the `SQL SECURITY DEFINER` procedure that wrap `http_request()` against it, and a second connection pointed at the workspace's own SCIM API. Revokes any `USE CONNECTION` left by a previous run, then asserts the caller holds nothing on the connection — without which row 8 cannot tell definer from invoker. |
| `setup/03_provision_delegated_job.py` | Uploads the notebook that performs the delegated action, creates the broker job with `run_as` the author, grants the caller `CAN_MANAGE_RUN`, and asserts the caller did *not* end up with `CAN_MANAGE`. |
| `setup/99_teardown.py` | Drops the schema cascade, drops all three connections, deletes the broker job and its notebook, and deletes every principal the matrix created — the two fixed targets, everything under the row 12 prefix, and the caller. Connections are metastore-level and the job is outside UC, so neither goes away with the schema. Run it before re-running row 4 or row 7 — both refuse to produce a result if their target principal already exists. |

## Matrix rows

| Script | Row | Question |
|---|---|---|
| `delegation/01_sql_udf_definer_vs_invoker.py` | 1 | Does a SQL UDF body run with the owner's privileges? Asserts the control (direct read denied) before trusting the wrapped read. |
| `delegation/02_python_udf_network_egress.py` | 2 | Can the Python sandbox reach the network? Probes the workspace control plane and a public endpoint, as both identities. |
| `delegation/03_python_udf_credential_context.py` | 3 | Does the sandbox hold a credential? Asks the sandbox to describe its own environment. |
| `delegation/04_end_to_end_create_service_principal.py` | 4 | Does the whole pattern work? Verdict is the SCIM directory read back as admin, not the function's return value. |
| `delegation/05_sql_surface_for_identity_admin.py` | 5 | Is identity administration expressible in SQL at all? Probed as admin — if the grammar is absent for an admin it is absent for everyone. |
| `delegation/06_function_body_readable_by_caller.py` | 6 | Can a caller with only `EXECUTE` read the body? Plants a sentinel (never a real secret) and tries three read paths. |
| `delegation/07_embedded_credential_end_to_end.py` | 7 | Does an author-embedded credential make it work? Uses the author's short-lived OAuth token, never logs the DDL, and drops the function in a `finally` block. |
| `delegation/08_connection_definer_vs_invoker.py` | 8 | Does a connection privilege resolve as definer? Three calls: the admin's (proves the connection works), the caller's direct call (proves it is unprivileged), the caller's call through the wrapper (the measurement). |
| `delegation/09_connection_credential_readable_by_caller.py` | 9 | Can a `USE CONNECTION` grantee read the stored token? Grants the privilege, proves the grant took effect, then probes four read paths for a sentinel. |
| `delegation/10_connection_grant_blast_radius.py` | 10 | What does `USE CONNECTION` authorise? Issues request shapes the wrapper never makes and separates platform refusals from origin responses — a 404 from the origin is the finding. |
| `delegation/11_control_plane_reachable_via_connection.py` | 11 | Is the workspace's own SCIM API reachable? Two probes — sentinel then live credential — because neither alone separates "unreachable" from "not allowed". Creates and drops its own connection; registers the token for redaction. |
| `delegation/12_job_run_as_delegated_action.py` | 12 | Can a `CAN_MANAGE_RUN` caller cause the shaped action? Verdict is the SCIM directory read back as admin, not the run's result state. Clears its own prior principals first so the read-back means this run. |
| `delegation/13_job_manage_run_boundary.py` | 13 | What does `CAN_MANAGE_RUN` withhold? Probes four ways to change what the job does, plus the one thing it does allow — reading run output. |
| `delegation/14_definer_procedure_vs_connection.py` | 14 | Does an explicit `SQL SECURITY DEFINER` procedure change row 8's answer? Revokes `USE CONNECTION` for the duration and restores it in a `finally` block so rows 9–10 stay reproducible in any order. |

## Reading the output

Failure output is data. `INSUFFICIENT_PERMISSIONS` (row 1's control),
`PARSE_SYNTAX_ERROR` (row 5), `HTTP 401` (row 2), and `User is missing USE
CONNECTION` (row 8) are each the finding for their row — *which* thing refused
and *how* is the result, so scripts print the rejected error body rather than
swallowing it. Rows 10 and 14 turn on distinctions between kinds of refusal:
Unity Catalog declining, `http_request()` rejecting an argument before dispatch,
and the origin answering are three different things and the scripts label them
separately.

Scripts are idempotent (`CREATE OR REPLACE` throughout) except where a
pre-existing target would make a result meaningless: rows 4 and 7 check the SCIM
directory first and record `inconclusive` rather than a false pass. Row 12 faces
the same hazard and resolves it the other way — it deletes principals left by its
own previous run, because refusing would make `make matrix` non-repeatable.

Two rows mutate state other rows depend on, and both are written so the order
does not matter: row 9 grants `USE CONNECTION` and leaves it granted (row 10
needs it), and row 14 revokes it and restores it in a `finally`. Re-running
`setup/02` revokes it too, so row 8 always starts from the state it needs.
