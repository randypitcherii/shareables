# scripts/

Every ✅/❌ in the README matrix is produced here. One numbered script per matrix
row; the number ties script → row → entry in `results/matrix_results.json`.

## Prerequisites

- A Databricks CLI profile for a **workspace admin** — the setup scripts create
  service principals and mint their OAuth secrets. SSO/OAuth only; `_common.py`
  exits if `DATABRICKS_TOKEN` looks like a `dapi…` personal access token.
- A **serverless** SQL warehouse. UC Python UDFs do not run on classic warehouses.
- A Unity Catalog catalog the profile can create schemas in.

## Shared modules

| File | Purpose |
|---|---|
| `_common.py` | Auth (`admin_client`, `lowpriv_client`), `.env` loading, SQL execution, results writing, redaction. |
| `delegation/_delegation_common.py` | Function invocation and JSON decoding for the matrix rows. |

Three things in `_common.py` are load-bearing and easy to break:

- **`lowpriv_client()` pins `auth_type="oauth-m2m"`.** The Makefile exports
  `DATABRICKS_CONFIG_PROFILE`, and without the pin the SDK's unified auth
  resolves the CLI profile first and silently returns the *admin* identity —
  turning every row into a self-test that always passes.
- **`assert_distinct_identities()` is called by every row that uses both
  identities.** It runs `SELECT current_user()` on each client and refuses to
  continue if they match. Configuration is not trusted; the warehouse is asked.
- **`write_result()` redacts.** Catalog name, workspace host, caller application
  id, and anything email-shaped are replaced with placeholders before anything
  is written, because raw platform error strings are exactly where real
  coordinates hide and `results/` is committed to a public repo.

## Setup and teardown

| Script | What it does |
|---|---|
| `verify.py` | One real request (`SELECT current_user()`, then `SHOW SCHEMAS`) to prove profile + warehouse + catalog before any matrix work. |
| `setup/00_create_lowpriv_principal.py` | Creates the caller service principal, mints a workspace OAuth secret, grants `CAN_USE` on the warehouse, asserts it is not in `admins`, writes the gitignored `.env`, and proves it authenticates as itself. |
| `setup/01_provision_objects.py` | Creates the schema, `sensitive_table`, the four probe functions, and grants the caller `USE CATALOG` / `USE SCHEMA` / `EXECUTE` — then asserts via `SHOW GRANTS` that the caller holds *nothing* on `sensitive_table`. |
| `setup/99_teardown.py` | Drops the schema cascade and deletes both target principals and the caller. Run it before re-running row 4 or row 7 — both refuse to produce a result if their target principal already exists. |

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

## Reading the output

Failure output is data. `INSUFFICIENT_PERMISSIONS` (row 1's control),
`PARSE_SYNTAX_ERROR` (row 5), and `HTTP 401` (row 2) are each the finding for
their row — *which* thing refused and *how* is the result, so scripts print the
rejected error body rather than swallowing it.

Scripts are idempotent (`CREATE OR REPLACE` throughout) except where a
pre-existing target would make a result meaningless: rows 4 and 7 check the SCIM
directory first and record `inconclusive` rather than a false pass.
