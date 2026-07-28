# Can a Unity Catalog UDF wrap one privileged action for less-privileged callers?

**The question.** A high-privilege admin defines a reusable UDF. A lower-privilege
user is granted permission to invoke it. The user should *not* receive the
underlying admin privilege directly. Can the function act as a narrowly scoped
wrapper around a single administrative action — creating a service principal,
concretely — rather than a broad grant?

**The answer, in one line.** For *data*, yes and it is a real boundary. For
*administrative actions*, no — not through the function model. It can be forced
to work by hardcoding a credential in the function body, and that hands the
credential to every caller in plaintext, so the workaround is worse than the
grant it replaces.

Run date: **2026-07-28**. One real AWS Databricks workspace, Unity Catalog
enabled, serverless SQL warehouse on the PRO channel. Author identity: a
workspace admin. Caller identity: an OAuth M2M service principal that is a plain
workspace user, holds `USE CATALOG` / `USE SCHEMA` / `EXECUTE` and `CAN_USE` on
the warehouse, and nothing else. Every row below was produced by a script in
`scripts/` writing to `results/matrix_results.json`; that file is the source of
truth and this table is its readable projection.

## Findings matrix

| # | Capability / Question | Claim / Source | Result | Notes |
|---|---|---|---|---|
| 1 | Does a UC **SQL** UDF body resolve underlying-object privileges as **definer** (owner) rather than invoker? | [SQL authorized user](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-authorized-user), [UC UDFs](https://docs.databricks.com/aws/en/udf/unity-catalog) | ✅ | Caller's direct read refused — `[INSUFFICIENT_PERMISSIONS] User does not have SELECT on Table ... SQLSTATE: 42501`. Same caller invoking the admin-owned UDF over the same table got `3`. `EXECUTE` sufficed; no `SELECT` was ever granted. |
| 2 | Can a UC **Python** UDF make an outbound HTTPS request? | [UC Python UDF limits](https://docs.databricks.com/aws/en/udf/python), [serverless network policies](https://docs.databricks.com/aws/en/security/network/serverless-network-security/network-policies) | ✅ | As the low-privilege caller: `https://example.com` → **HTTP 200**; workspace control plane → **HTTP 401**. A 401 is an authorization answer, so the request completed the round trip. **Environment-dependent** — see caveat below. |
| 3 | Does a UC Python UDF receive an ambient Databricks credential? | field question | ❌ | The sandbox exports exactly one `DATABRICKS_*` variable, `DATABRICKS_ROOT_VIRTUALENV_ENV`, which authenticates nothing. No token, host, or client id. The SDK imports but `WorkspaceClient()` raises `default auth: cannot configure default credentials`. `dbutils` absent, no active Spark session. Identical for admin and caller. |
| 4 | End to end: can a low-privilege caller create a service principal through an admin-owned UDF? | the customer's actual proposal | ❌ | Caller invoked the wrapper; the SCIM directory read back as admin afterwards contained no such principal. Failure is capability (rows 2–3), not authorization — the invocation itself was permitted. |
| 5 | Is there any **SQL surface** for identity administration a definer-rights SQL UDF could wrap? | inferred from row 1 | ❌ | `CREATE SERVICE PRINCIPAL`, `CREATE USER`, `CREATE LOGIN` all `[PARSE_SYNTAX_ERROR] ... SQLSTATE: 42601` for a workspace admin. The grammar does not exist. |
| 6 | Can a caller holding **only `EXECUTE`** read the function body, and therefore any credential embedded in it? | the obvious workaround to rows 3–5 | ❌ | The caller read the body via **`DESCRIBE FUNCTION EXTENDED`** and via **`information_schema.routines`**. A planted sentinel string was visible through both. (`SHOW CREATE TABLE` was refused — `TABLE_OR_VIEW_NOT_FOUND`.) The caller could still invoke, so it genuinely held only `EXECUTE`. |
| 7 | Does embedding the author's credential in the body make the delegated action succeed? | the workaround, tested | ✅ | Caller holding only `EXECUTE` invoked the wrapper; SCIM returned **HTTP 201** and the principal was present in the directory on read-back. Works — via a stored secret, not via the function model. Function dropped immediately; token never written to `results/`. |

Status vocabulary: ✅ works as claimed · ❌ does not, with evidence ·
◑ partially · ❓ could not be isolated.

## Key findings

### The two halves of the pattern never meet

Row 1 is a genuine, useful result and it is worth separating from the rest: a UC
SQL UDF **is** a privilege boundary. The caller was refused `SELECT` on
`sensitive_table` and then read it — indirectly, through the shape the author
chose — with nothing but `EXECUTE`. That is exactly the delegation the question
describes, and for data access it works.

It does not extend to administrative actions, because of a gap that has nothing
to do with the privilege model:

- The function type that **carries definer's rights** (SQL) cannot express
  "create a service principal" — there is no such statement in the dialect
  (row 5).
- The function type that **can express it** (Python, via REST) has network reach
  (row 2) but no identity to spend (row 3). It is anonymous inside the sandbox.

So the honest answer to "does the function model support this kind of delegated
action" is: **the function model never gets asked.** The Python UDF is not
denied by Unity Catalog — it simply has no credential to present, and the
control plane answers `401` like it would to any anonymous caller. Row 4 is the
consequence, not an independent finding.

### The credential workaround inverts the security property

Rows 6 and 7 are the ones to put in front of a customer, because they are where
someone determined to make this work ends up.

Embedding the author's token in the function body **does** work (row 7): a caller
with only `EXECUTE` created a service principal, HTTP 201, verified by reading
the SCIM directory back as admin. The action really is narrow — the caller can
pass only a display name.

But the elevation is not coming from the function model. The platform never
re-authorizes the body as the owner; it replays whatever secret the author left
in the source. And row 6 shows the caller can read that source: both
`DESCRIBE FUNCTION EXTENDED` and `information_schema.routines` returned the body
text, sentinel included, to a principal holding nothing but `EXECUTE`.

The wrapper therefore does not narrow anything. A caller who can invoke it can
also extract the credential and call the SCIM API directly with the **full**
admin privilege, not the one action the author intended to expose. That is
strictly worse than granting the narrow privilege outright, and it fails open —
silently, whenever someone runs `DESCRIBE`.

It also expires. The embedded token is short-lived, so the wrapper stops working
at a time unrelated to any change anyone made to it.

### Viability verdict

| Delegation target | Viable via UDF? | Why |
|---|---|---|
| Reading data the caller lacks `SELECT` on | **Yes** | Definer's rights, row 1. Sound and supported. |
| Any administrative action (identity, workspace config, ACLs) | **No** | No SQL surface (row 5); Python sandbox has no identity (row 3). |
| Same, forced with an embedded credential | **No — actively harmful** | Works (row 7) but discloses the credential to every caller (row 6). |

For the administrative case the primitive to reach for is one that has a real
run-as identity of its own — a job or Databricks App running as a service
principal that holds the narrow privilege — rather than a function. That keeps
the credential outside anything the caller can read. Scoping that alternative is
outside this experiment; the point here is only that the UDF route is closed.

## Caveats and scope

- **Row 2 is a property of this workspace's egress posture, not of UC.** Databricks
  documents serverless egress as deny-by-default under a
  [network policy](https://docs.databricks.com/aws/en/security/network/serverless-network-security/network-policies);
  this workspace evidently runs a permissive one, which is why `example.com`
  answered. A workspace with a restrictive policy would see row 2 fail. That
  makes the overall conclusion *stronger*: even in the most permissive egress
  posture available, the pattern still fails without an embedded secret.
- **Row 6 is a negative scoped to the paths probed** — `DESCRIBE FUNCTION EXTENDED`,
  `SHOW CREATE TABLE`, `information_schema.routines`. Two of the three leaked, so
  the finding stands regardless, but this is not a claim that those are the only
  read paths.
- Rows 1–6 come from a single provisioning run; row 7 re-ran after a teardown fix
  against the same provisioned objects. Timestamps are in
  `results/matrix_results.json`.
- Not tested: Scala/Java UDFs, UDFs on classic (non-serverless) compute,
  UC Connections or service credentials as an alternative credential source
  inside the sandbox.

## Running it

```bash
make                         # list targets
make verify                  # one real request — proves auth, warehouse, catalog
make setup                   # low-privilege caller + admin-owned objects + grants
make matrix                  # all seven rows -> results/matrix_results.json
make teardown                # remove schema, functions, and both service principals
```

Configure via env vars (all echoed by `make help`): `DATABRICKS_CONFIG_PROFILE`,
`EXPERIMENT_CATALOG`, `EXPERIMENT_SCHEMA`, `EXPERIMENT_WAREHOUSE_ID`.

The experiment needs a workspace admin profile (it creates service principals
and mints their OAuth secrets) and a serverless SQL warehouse. Authentication is
SSO/OAuth only — `scripts/_common.py` refuses a `dapi…` personal access token on
sight. Real coordinates are redacted at the results-write boundary, so committed
evidence carries placeholders only.
