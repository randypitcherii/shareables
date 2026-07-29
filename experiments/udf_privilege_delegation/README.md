# Can one privileged action be wrapped for less-privileged callers?

**The question.** A high-privilege admin defines something reusable. A
lower-privilege user is granted permission to invoke it. The user should *not*
receive the underlying admin privilege directly. Can that thing act as a narrowly
scoped wrapper around a single administrative action — creating a service
principal, concretely — rather than a broad grant?

**The answer, in one line.** Not through Unity Catalog's function model, and not
through a UC connection either — but yes through a **job whose `run_as` identity
holds the privilege**, where the credential lives on the compute and the
constraints live in code the caller can neither read nor edit.

The matrix runs in three parts. Rows 1–7 (2026-07-28) close the UDF route. Rows
8–11 and 14 (2026-07-29) close the UC connection route, which looked like the
closest fit and is not one. Rows 12–13 (2026-07-29) measure the route that works.
The ranked write-up of every mechanism surveyed, including the five not tested
live, is in
[docs/research/2026-07-29-service-principal-delegation-options.md](docs/research/2026-07-29-service-principal-delegation-options.md).

Run dates: **2026-07-28** (rows 1–7) and **2026-07-29** (rows 8–14). One real AWS
Databricks workspace, Unity Catalog enabled, serverless SQL warehouse on the PRO
channel, IP access lists enabled. Author identity: a workspace admin. Caller
identity: an OAuth M2M service principal that is a plain workspace user, holds
`USE CATALOG` / `USE SCHEMA` / `EXECUTE` and `CAN_USE` on the warehouse, plus
`CAN_MANAGE_RUN` on one job, and nothing else. Every row below was produced by a
script in `scripts/` writing to `results/matrix_results.json`; that file is the
source of truth and this table is its readable projection.

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
| 8 | Does a SQL UDF resolve **`USE CONNECTION`** on an admin-owned connection as **definer**, the way it resolved table `SELECT`? | [http_request](https://docs.databricks.com/aws/en/sql/language-manual/functions/http_request), [CREATE CONNECTION](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-connection) | ❌ | **Invoker.** Caller holding `EXECUTE` on the wrapper and nothing on the connection got the *same* refusal it got calling directly — `PERMISSION_DENIED … User is missing USE CONNECTION on <connection>`. Control: the admin's identical call returned **200**. Definer's rights cover what a body reads, not the connection it calls out through. |
| 9 | Can a caller granted `USE CONNECTION` read the credential the connection stores? | the row 6 question, asked of the other primitive | ✅ | **The credential stays hidden.** With the grant held and demonstrably in use, `DESCRIBE CONNECTION`, `SHOW CONNECTIONS`, `system.information_schema.connections` and the connections REST API were all allowed and none returned the sentinel. `DESCRIBE` reports `auth_scheme -> bearer` and the host, and stops. This is the one thing rows 6–7 could not deliver. |
| 10 | Is `USE CONNECTION` scoped to the author's call, or to the whole connection? | inferred from row 8 | ❌ | **The whole connection.** Holding it, the caller reached the origin on every shape the wrapper never makes: `GET` on another path (**404**), `POST` (**405**), `DELETE` (**405**) — each answered by the far end, which is what proves the platform dispatched it. Only the host and `base_path` bind: `../../etc/passwd` was rejected `INVALID_HTTP_REQUEST_PATH`. |
| 11 | Can `http_request()` reach *this* workspace's own SCIM API? | prerequisite for the whole connection family | ❌ | **Reached, then refused.** Sentinel credential → **401** about the credential, so the round trip completes. Same call with a valid credential → **403** from the workspace's IP access list, naming the egress address. Environment-dependent, and independent of every privilege question. |
| 12 | Can a caller holding only `CAN_MANAGE_RUN` cause a service principal to be created, in a shape the author fixed? | the customer's requirement, aimed at a job | ✅ | Caller was refused the SCIM create directly (`PermissionDenied`), then triggered the job and `udf-delegation-job-sp-alpha` was present on admin read-back — the prefix composed by the notebook, not supplied by the caller. Suffix `../Evil Name` was rejected by the notebook's rule and created nothing. |
| 13 | Does `CAN_MANAGE_RUN` withhold the ability to change what the job does? | the row 6 question, asked of the job | ✅ | Repointing the task, changing `run_as`, and self-granting `CAN_MANAGE` were all `PermissionDenied`; reading the notebook came back `ResourceDoesNotExist`. Reading the output of runs it triggered **is** allowed — by design, and the operational rule that follows from it. |
| 14 | Does an explicit **`SQL SECURITY DEFINER` procedure** change row 8's answer? | extension — procedures state the security mode outright | ◑ | Changes the answer without delivering the outcome. The caller was **not** refused for missing `USE CONNECTION`, so the clause clears that check; the call then failed at credential resolution — **401** `Credential was not sent or was of an unsupported type`, carrying a Databricks request id. Measured behaviour, not a documented contract. |

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

### A connection hides the credential and still does not wrap the action

The UC HTTP connection is the primitive Unity Catalog offers for exactly the
problem row 6 exposed, and it solves that problem. Row 9 granted the caller
`USE CONNECTION`, confirmed it was really using the connection, and then failed
to extract the stored token through four different read paths. That is the
property the embedded-credential workaround could not provide, and it is worth
stating plainly because it is the reason someone will reach for a connection.

Then it fails on the other half. Row 8: the connection privilege resolves as
**invoker**, so a caller holding only `EXECUTE` on an admin-owned wrapper gets
the identical refusal it gets calling `http_request()` itself. Definer's rights
cover the objects a body *reads* — that is row 1, and it still holds — not the
connection a body *calls out through*.

Because row 8 forces the caller to hold `USE CONNECTION` directly, row 10 becomes
the question that matters, and its answer is that the grant is scoped to the
connection rather than to the author's call. The caller reached the origin with a
different path, with `POST`, and with `DELETE` — none of which the wrapper makes.
The only thing that binds is the connection's host and `base_path`.

For the concrete use case that is the whole story: a connection pointed at
`/api/2.0/preview/scim/v2/` does not delegate *create one service principal under
a fixed prefix*. It delegates the SCIM API — create, enumerate, delete, users as
well as principals — to everyone granted `USE CONNECTION`, whatever the wrapper
says. **Use a connection to hide a credential. Do not expect it to shape an
action.**

Row 14 is the same finding from an unexpected angle. A `SQL SECURITY DEFINER`
stored procedure — where the author states the intent in the object definition,
which the function model gave no way to do — gets *past* the `USE CONNECTION`
check and then fails at credential resolution with a platform-emitted 401. Not a
delegation, not the function's refusal, and not described anywhere in the public
documentation, so it is recorded as a measurement rather than a contract.

### The job is the primitive that has all three parts

What every earlier row lacks is a single place that has an identity, a credential
the caller cannot read, and an enforcement point the caller cannot reach. A job
has all three, and rows 12–13 measured it rather than assuming it.

Row 12: the caller was refused the SCIM create directly, triggered a job it holds
only `CAN_MANAGE_RUN` on, and a service principal existed afterwards under a name
the notebook composed. The suffix `../Evil Name` was rejected by the notebook's
own rule and created nothing — so the constraint is code running as the
privileged identity, not a convention the caller is trusted to follow.

Row 13 is the row 6 question asked of the job, and it comes back the other way.
The caller could not repoint the task, could not change `run_as`, could not grant
itself `CAN_MANAGE`, and could not read the notebook. The one thing
`CAN_MANAGE_RUN` does grant is reading the output of runs it triggers — by
design, and worth designing around: **whatever the job prints, the caller reads.**

The failure mode to guard is the permission itself. `CAN_MANAGE` instead of
`CAN_MANAGE_RUN` lets the holder repoint the task, which converts one narrow
action into the run-as identity's full privilege. That is one dropdown away, and
nothing in the platform will flag it.

### Viability verdict

| Delegation target | Mechanism | Viable? | Why |
|---|---|---|---|
| Reading data the caller lacks `SELECT` on | SQL UDF | **Yes** | Definer's rights, row 1. Sound and supported. |
| Any administrative action | SQL / Python UDF | **No** | No SQL surface (row 5); Python sandbox has no identity (row 3). |
| Same, forced with an embedded credential | UDF + hardcoded token | **No — actively harmful** | Works (row 7) but discloses the credential to every caller (row 6). |
| Hiding a credential from the principals that use it | UC HTTP connection | **Yes** | Row 9. Four read paths, none disclosed it. |
| Constraining *which* call those principals may make | UC HTTP connection | **No** | Invoker rights (row 8); the grant covers the connection, not the call (row 10). A definer procedure gets past the check and still fails (row 14). |
| An administrative action, shaped and delegated | Job with `run_as` | **Yes** | Rows 12–13. Action delivered, shape enforced in code, every edit path refused. |

The ranked survey of every mechanism considered — including the five judged from
documentation rather than measured — is in
[docs/research/2026-07-29-service-principal-delegation-options.md](docs/research/2026-07-29-service-principal-delegation-options.md).

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
  read paths. **Row 9 is a positive with the same scope limit** and matters more
  for it: four read paths returned no credential, which is not a proof that none
  exists.
- **Rows 8–10 and 14 point at `example.com`, not at the SCIM API.** That is
  deliberate. Row 11 shows this workspace's IP access list refuses the
  authenticated self-referential call, so a SCIM target would have answered 403
  for a reason unrelated to privilege and the definer/invoker question would
  never have been reached. The privilege finding is therefore measured cleanly;
  what it does *not* license is any claim about SCIM behaving differently, and
  nothing here assumes it would.
- **Row 11 is about this workspace, not about Unity Catalog.** A workspace
  without IP access lists, or one whose list covers the serverless egress
  address, would not see it.
- **Row 14 is a measurement, not a documented contract.** No public documentation
  describes how a `SQL SECURITY DEFINER` procedure resolves a connection
  credential; this is one workspace on one date, and it is the kind of edge that
  changes without notice.
- **`http_request` is deprecated.** Databricks now points at the
  [Unity Catalog connections proxy endpoint](https://docs.databricks.com/aws/en/query-federation/http)
  for new code. It takes the same `USE CONNECTION` grant, so rows 9 and 10 are
  expected to carry over — credential hidden, grant scoped to host and
  `base_path` — but that was not measured here.
- Rows 1–14 come from a single `make setup && make matrix` run on 2026-07-29;
  timestamps are in `results/matrix_results.json`.
- Not tested: Scala/Java UDFs, UDFs on classic (non-serverless) compute, the
  connections proxy endpoint, Databricks Apps, and every desk-researched
  candidate in the research document.

## Running it

```bash
make                         # list targets
make verify                  # one real request — proves auth, warehouse, catalog
make setup                   # caller + objects + connections + broker job + grants
make matrix                  # all fourteen rows -> results/matrix_results.json
make teardown                # remove everything: schema, connections, job, principals
```

Configure via env vars (all echoed by `make help`): `DATABRICKS_CONFIG_PROFILE`,
`EXPERIMENT_CATALOG`, `EXPERIMENT_SCHEMA`, `EXPERIMENT_WAREHOUSE_ID`.

The experiment needs a workspace admin profile (it creates service principals
and mints their OAuth secrets) and a serverless SQL warehouse. It also needs
`CREATE CONNECTION` on the metastore for rows 8–11 and 14. Authentication is
SSO/OAuth only — `scripts/_common.py` refuses a `dapi…` personal access token on
sight. Real coordinates are redacted at the results-write boundary — catalog,
host, caller id, anything email-shaped, any IPv4 address, and any credential a
row registers — so committed evidence carries placeholders only.

Connections live in a flat metastore-wide namespace and the broker job lives
outside Unity Catalog, so neither is removed by dropping the schema. `make
teardown` removes both explicitly, along with every principal the matrix created.
