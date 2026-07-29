---
title: "Constrained delegation paths for service-principal creation"
tags: [unity-catalog, service-principals, delegation, identity, jobs, connections]
status: active
created: 2026-07-29
---

# Constrained delegation paths for service-principal creation

## The question

An under-privileged principal needs to cause service principals to be created —
dynamically, and in a shape somebody else fixed: constrained naming, constrained
entitlements and group membership, constrained quantity, auditable, revocable.
It must never handle, and never need to be trusted with, a credential. That rules
out the whole secret-in-the-loop family: tokens embedded in function bodies,
caller-supplied secret arguments, secret scopes read at call time, UC secrets.

The prior experiment ([README](../../README.md), rows 1–7) closed the obvious
route. A Unity Catalog UDF cannot do it: the function type that carries definer's
rights has no grammar for identity administration, the function type that could
express it has no identity to spend, and forcing it with an embedded credential
discloses that credential to every caller. This document ranks what is left.

**What was tested live vs. sourced from documentation.** Rows 8–14 of the
findings matrix are live measurements against one real AWS workspace on
2026-07-29, produced by scripts in `scripts/delegation/` writing to
`results/matrix_results.json`. Those rows cover candidates 1 and 2 — the UC
connection family and the job-as-broker family. Every other candidate below is
desk research against public Databricks documentation, and is labelled as such
in its section. Where a section makes a claim that was measured, it cites the
matrix row.

## The recommendation, in two sentences

**Broker the action through a Databricks job whose `run_as` identity holds the
privilege, and grant the caller `CAN_MANAGE_RUN` and nothing else.** The
credential lives on the compute the job runs on, where the caller has no read
path to it; the naming, entitlement and quantity constraints live in the job's
code, which `CAN_MANAGE_RUN` does not permit the caller to read or change; and
the caller's interface is a job parameter.

That is the only candidate surveyed that supplies all three properties at once —
a real execution identity, an enforcement point the caller cannot reach, and a
narrow interface — and rows 12 and 13 measured it end to end rather than
inferring it.

## Ranked shortlist

| Rank | Mechanism | Credential hidden? | Shape enforced where? | Evidence | Status |
|---|---|---|---|---|---|
| 1 | **Job with `run_as` a service principal** | Yes — on the compute | Job code (row 13: caller cannot read or edit it) | Live, rows 12–13 | GA |
| 2 | Databricks App running as its own SP | Yes — injected env vars | App code | Docs | GA |
| 3 | Pre-provisioned pool + native delegated roles | N/A — no creation at call time | Pool size, group rules | Docs | GA / Public Preview |
| 4 | IdP-driven SCIM provisioning | Yes — outside Databricks | IdP group rules | Docs | GA |
| 5 | Terraform / CI with an approval gate | Yes — in CI | Code review + module | Docs | GA |
| 6 | Model Serving endpoint / agent tool | Yes | Endpoint code | Docs | GA |
| 7 | UC HTTP connection + `http_request()` | **Yes** (row 9) | **Nowhere** (rows 8, 10, 14) | Live, rows 8–11, 14 | Deprecated |
| — | A native fine-grained role for creation | — | — | Docs + row 5 | Does not exist |

Ranks 1–2 are runtime delegation. Ranks 3–5 relocate the problem, which for many
requirements is the better answer. Rank 6 is rank 1's shape with more moving
parts. Rank 7 is refuted as a delegation primitive and kept in the list because
the refutation is the useful part.

---

## 1. Job with `run_as` a service principal — recommended

**Tested live.** Matrix rows 12 and 13.

An admin creates a job whose task is the delegated action and whose `run_as`
identity holds the privilege. The caller is granted `CAN_MANAGE_RUN`, which
permits triggering a run and reading its output, and nothing else. The caller's
only input is a job parameter.

**Does the credential stay unreadable?** Yes, and for a better reason than
"nobody granted it": there is no credential to read. The job's identity is
attached to the compute at run time by the platform. Row 13 measured the four
ways a caller might reach around it — repoint the task at different code, change
the `run_as` identity, grant itself `CAN_MANAGE`, read the notebook the job runs
— and all four were refused. That last probe is row 6 asked of the new object,
and it comes back the other way: the logic the delegation depends on is not
readable by the population allowed to trigger it.

**How are shape constraints enforced?** In the job's code, which is exactly where
the caller cannot reach. In the tested implementation the caller supplies a
suffix; the notebook validates it against `^[a-z0-9-]{1,24}$`, prepends a fixed
prefix, sets the entitlements itself, and creates exactly one principal. Row 12
triggered it twice — once with a well-formed suffix, which produced a principal
the admin then found in the SCIM directory under the composed name, and once
with `../Evil Name`, which the notebook rejected and which created nothing.

Quantity, group membership and tags are the same kind of constraint: they are
whatever the code says, and the code is not the caller's to change.

**What must the caller be granted?** `CAN_MANAGE_RUN` on one job. Nothing on the
catalog, nothing on the identity APIs, no entitlements.

**GA / Preview?** GA. Jobs, `run_as`, and job ACLs are all long-standing.

**Latency and operational cost.** A job run — seconds to a couple of minutes
depending on compute warm-up, so this is an asynchronous request/fulfil
interface, not a synchronous API. Operationally it is one job and one privileged
identity to review.

**What breaks it.**

- **`CAN_MANAGE` instead of `CAN_MANAGE_RUN`.** `CAN_MANAGE` lets the holder
  repoint the task, so the grant stops being one action and becomes the run-as
  identity's full privilege. This is the single mistake that inverts the whole
  design, and it is one dropdown away.
- **Anything the job prints.** Row 13 confirmed the caller can read the output of
  runs it triggers. That is by design and it is fine — until someone logs a
  token, a connection string, or another principal's details. Job output is part
  of the caller's surface even though the job's credential is not.
- **Parameter handling in the job code.** The enforcement point is code, so it
  fails the way code fails. The suffix is concatenated into an identity name; if
  it were concatenated into a SQL statement, an API path, or a shell command, the
  narrow interface would be a wide one. Validate against an allowlist pattern,
  never a denylist.
- **The standing privileged identity.** The `run_as` identity holds the
  privilege permanently, so it is worth scoping to the minimum that works and
  reviewing like any other admin credential.
- **Auditability depends on reading two records.** The job run history attributes
  the trigger to the caller; the identity-management audit events attribute the
  creation to the `run_as` identity. Neither alone tells the whole story.

---

## 2. Databricks App running as its own service principal

**Desk research.** Public documentation; not measured here.

Structurally the same trust argument as the job, with a synchronous interface. An
app gets a dedicated service principal — Databricks documents it as "unique to
the app instance", stable across deployments, and deleted with the app — and the
platform "automatically injects service principal credentials into the app's
environment", so the credential is server-side and not exposed to end users. End
users are granted `CAN_USE` on the app.

The difference that matters is authorization granularity. The documentation is
explicit that app authorization "doesn't support user-level access control. All
users who interact with the app share the same permissions defined for the
service principal." So per-caller rules — who may create how many, under which
prefix — have to be implemented in the app's own code against the authenticated
user identity the app receives. The on-behalf-of-user mode does not help here,
because the whole premise is that the user does *not* hold the privilege.

Choose this over the job when the requester needs an answer immediately, a form
rather than a parameter, or a UI. Choose the job when an asynchronous interface
is acceptable, because the job needs no always-on service to operate and has less
code in the trusted position.

---

## 3. Pre-provisioned pool plus native delegated roles

**Desk research.** Public documentation, plus matrix row 5.

This candidate is not on the original list and it deserves to be, because it is
the one that dissolves the problem rather than solving it.

Databricks has no role that lets a non-admin *create* a service principal — see
section 7 — but it does have real non-admin roles over principals that already
exist:

- **Service Principal Manager**, which manages roles on a service principal. The
  creator holds it on what they created, and account admins hold it on
  everything.
- **Service Principal User**, which allows running jobs as that service
  principal.
- **`Manage` on a group** (Public Preview), which lets a non-admin manage group
  membership, delete the group, and pass those permissions on. Group *creation*
  still requires an admin.

So an admin can create a pool of N principals up front under the naming
convention, and delegate membership and assignment to a non-admin. "Constrained
quantity" becomes the pool size, "constrained naming" becomes the names the admin
chose, and "constrained entitlements" becomes group membership the delegate is
natively allowed to manage.

Worth asking before building anything: is the requirement genuinely *creation*,
or is it "a team needs identities allocated to it without waiting on an admin"?
If it is the second, this is a supported answer today with no broker in the path.
It fails only where the number of principals is genuinely unbounded, or where
each must be created at an unpredictable moment.

---

## 4. IdP-driven SCIM provisioning

**Desk research.** Public documentation; not measured here.

The identity lifecycle moves out of Databricks entirely: Okta or Entra ID owns
creation, and group-membership rules in the IdP drive what lands in Databricks
and with which entitlements. Naming and quantity are governed by the IdP's own
provisioning rules; revocation is deprovisioning; the audit trail is the IdP's.

This is the strongest governance story of anything here, and it is the right
answer whenever the organisation already runs SCIM provisioning — which, at the
kind of organisation that asks this question, it usually does.

It is ranked below the brokers because it answers a slightly different question.
It does not give a low-privilege *Databricks* principal a runtime path; it
relocates the privilege to whoever administers the IdP. That is an improvement in
governance and often a step backwards in self-service latency. Where the
requester's workflow lives in Databricks and needs an identity mid-flight, a
broker still has to sit in front of it.

---

## 5. Terraform or a CI pipeline with an approval gate

**Desk research.** Public documentation; not measured here.

The requester opens a pull request against a repository that owns the service
principals as code; CI holds the credential; a reviewer approves; the pipeline
applies. Constraints are enforced twice — by the module's interface, which can
accept only a suffix and derive everything else, and by code review.

The credential never leaves CI, the audit trail is the git history, and
revocation is a revert. It is the most auditable option here and the least
dynamic: the round trip is a human review, so it is minutes to days rather than
seconds.

Use it when creation is rare, consequential, or needs a second pair of eyes. It
composes well with the job broker rather than competing with it — the same
constraint logic can live in a Terraform module for planned creation and in job
code for runtime creation.

---

## 6. Model Serving endpoint or agent tool as an action broker

**Desk research.** Public documentation; not measured here.

A served model or agent endpoint runs with an identity of its own and can expose
a shaped tool. As a trust argument this is rank 1 with more moving parts: the
credential is server-side, the constraint is in the endpoint's code, the caller
is granted invocation on the endpoint.

It is ranked last of the working options because it adds a serving stack to a
problem that does not need one. It is worth reaching for only when the delegated
action already belongs to an agent workflow — when a model is going to decide
that a principal is needed. Note also that Databricks now steers agent tooling
toward MCP services and the Unity Catalog connections proxy for reaching external
services rather than toward UC functions wrapping `http_request`, so an agent
route should be designed against the current guidance rather than against the
function-tool pattern.

---

## 7. UC HTTP connection and `http_request()` — refuted

**Tested live.** Matrix rows 8, 9, 10, 11 and 14. This was the most promising
candidate on the original list and the one worth being precise about, because it
gets one thing exactly right and still does not work.

The idea: a connection object holds the credential, an admin-owned SQL UDF calls
`http_request()` against it with the method and path fixed, and callers are
granted only `EXECUTE`. If the connection privilege resolved as **definer** — the
way table `SELECT` did in row 1 — this would be the original proposal, working.

**It resolves as invoker (row 8).** The caller, holding `EXECUTE` on the
admin-owned function and nothing on the connection, was refused with the same
message it got calling the connection directly: *User is missing USE CONNECTION
on …*. Definer's rights cover the objects a function body reads; they do not
cover the connection a function body calls out through.

**The credential really is hidden (row 9).** This is the one property the
mechanism delivers, and it is the property the embedded-credential workaround
could not. With `USE CONNECTION` granted and demonstrably in use, none of
`DESCRIBE CONNECTION`, `SHOW CONNECTIONS`,
`system.information_schema.connections`, or the connections REST API returned the
token. `DESCRIBE` reports `auth_scheme -> bearer` and the host and stops.

**But the grant is connection-wide, not call-wide (row 10).** Because row 8
forces the caller to hold `USE CONNECTION` directly, the question becomes what
that grant authorises — and it authorises the connection, not the author's call.
Holding it, the caller reached the origin on every request shape the author's
function never makes: a different path, a `POST`, a `DELETE`. Each was answered
by the far end rather than refused by the platform, which is the proof that the
platform let it out. The one boundary that does hold is the connection's host and
`base_path`: a path that tried to climb above it was rejected outright with
`INVALID_HTTP_REQUEST_PATH`.

The consequence for the original use case is the whole finding. A connection
pointed at `/api/2.0/preview/scim/v2/` does not delegate "create one service
principal under a fixed prefix". It delegates the SCIM API — creating,
enumerating, and deleting principals and users alike — to everyone granted
`USE CONNECTION`, whatever the wrapper function says.

**A `SQL SECURITY DEFINER` procedure changes the answer without fixing it
(row 14).** Stored procedures require the security mode as an explicit clause,
so an author can state the intent the function model could not. Called by the
low-privilege caller, the procedure was *not* refused for missing
`USE CONNECTION` — so the clause is honoured far enough to clear that check — and
then failed at credential resolution with a platform-shaped 401, *Credential was
not sent or was of an unsupported type*. That is neither a working delegation nor
the function's refusal. It is recorded as a measured behaviour of one workspace
on one date, not as a contract: no public documentation describes this path.

**Reachability is its own problem (row 11).** On a workspace with IP access lists
enabled, a UC connection pointed at the workspace that hosts it makes a call that
leaves and re-enters the perimeter. The sentinel probe proved the request
completes the round trip — the control plane answered about the credential — and
the same call with a valid credential was then refused by the IP access list,
naming the egress address `http_request()` presents. This is environment-specific
and independent of every privilege question, and it is the kind of thing that
surfaces after the design is finished rather than before.

**Finally, `http_request` is deprecated.** Databricks documents the Unity Catalog
connections proxy endpoint as its replacement:
`/api/2.0/unity-catalog/connections/<name>/proxy[/<sub-path>]`, requiring
`USE CONNECTION` on the connection, with the credential injected server-side.
That is the supported way to get the row 9 property, and it inherits row 10's
shape: the grant is scoped to the connection's host and `base_path`, not to one
action. **Use a connection to hide a credential. Do not expect it to shape an
action.**

---

## 8. A native fine-grained role for creating service principals — does not exist

**Desk research plus matrix row 5.**

There is no account-level or workspace-level role short of admin that can create
a service principal. Creation is an account admin action in the account console
and a workspace admin action in a workspace. The roles that do exist —
Service Principal Manager, Service Principal User, and group `Manage` — all
operate on principals and groups that already exist, which is what makes section
3 possible and this section short.

Row 5 is the SQL-side half of the same answer: `CREATE SERVICE PRINCIPAL`,
`CREATE USER` and `CREATE LOGIN` are all `PARSE_SYNTAX_ERROR` even for a
workspace admin. The grammar does not exist, so no SQL-layer object — function or
procedure — can wrap it.

If a native constrained-creation role is something the organisation wants, the
honest answer is that it is a product request, and the mechanisms above are what
bridges the gap in the meantime.

---

## What this changes about the earlier conclusion

The prior README ended by pointing at "a job or Databricks App running as a
service principal" as the primitive to reach for, and called scoping it out of
scope. That instinct was right and is now measured: rows 12 and 13 show the job
route delivering the action end to end, with the caller refused every path to the
code, the identity, and the permissions behind it.

The new information is about the connection family, which looked like a closer
fit and is not one. It solves the credential-disclosure problem that killed rows
6 and 7 — genuinely, and that is worth knowing — and then fails on the half of
the problem the function model was supposed to solve, because `USE CONNECTION`
grants the connection rather than the call. A design that reaches for a
connection to hide a secret is sound. A design that reaches for one to constrain
an action is not, and the failure is silent: the wrapper compiles, the grants
look narrow, and the caller can go around them.
