# The One True Way (for dbt on Databricks)

> ⚠️ **DRAFT — still being refined for final publishing.**
> This is a working synthesis of the philosophy, captured from the reference
> implementation in this folder plus prior writing. Wording, ordering, and the set
> of principles are not final. Do not treat this as the published canon yet.

---

## What this is

"The One True Way" is an opinionated, batteries-included way to run a dbt project on
Databricks so that it is **safe to clone, trivial to onboard, impossible to step on a
teammate, and ruthless about correctness** — without anyone needing to run `dbt init`,
copy a secret, or read a wiki first.

The north star is **release confidence, not release hesitation**: anyone — a new hire or
an AI agent — should be able to make a change and trust the path from dev → CI → prod,
because isolation and testing are built in, not bolted on.

The thesis: **environment isolation and zero-config onboarding are not advanced topics you
bolt on later. They are the foundation you start from.** Environments map to dev / test /
prod (Unity Catalog catalogs, typically within a single workspace); the schema-routing macro
keeps everyone out of each other's way; and dbt deployed via Databricks Asset Bundles (DABs)
is the delivery mechanism. Get that right on day one and everything else (testing, CI, docs,
grants) has a clean place to live.

---

## Diagrams (draft)

Browser-openable visuals for the concepts below live in [`docs/diagrams/`](docs/diagrams/)
(open the `.html` files locally). Flagship: **One model, three environments** — the deployment
lifecycle. These are drafts, refined alongside this document.

## The principles

### 1. Configuration is environment-driven, with public dev fallbacks
Every environment-specific value is `env_var('NAME', '<dev fallback>')`. The dev
fallback lives in source control so a brand-new contributor can clone and `dbt build`
with **zero configuration**. Non-dev deployments inject real values via environment
variables.

- **Why:** the biggest tax on a dbt project is the "works on my machine after 30 minutes
  of setup" onboarding. Fallbacks-in-source removes it entirely.
- **In this repo:** `deployment_environment`, `default_catalog`, `default_schema` in
  `dbt_project.yml`, each `env_var(..., '<fallback>')`.

### 2. ...but some things must NOT have a fallback
`host` (workspace URL) and `http_path` (warehouse id) get **no** in-repo fallback. They
are workspace-specific (no universal default exists) and they are internal infrastructure
identifiers that should not be published. Require them explicitly.

- **Why:** a fallback you publish is a fallback you've decided to expose. Workspace URLs
  and warehouse ids fail both tests: useless to others, and needless internal disclosure.
- **In this repo:** `profiles.yml` reads `DBT_HOST` / `DBT_HTTP_PATH` with no default; a
  committed `template.env` documents them. `catalog`/`schema` keep dev fallbacks.

### 3. Commit `profiles.yml` — dev auth is SSO with no stored secret
The project's `profiles.yml` is version-controlled. The `dev` target uses OAuth U2M
(SSO browser login), which stores **no secret**, so committing it is safe. CI/prod
targets use M2M OAuth with every value (and the secret) from env vars.

- **Why:** the moment connection config is "your job to create," onboarding breaks and
  configs drift. A committed profile with SSO dev auth is the paved path.
- **In this repo:** one committed `profiles.yml`, three targets (`dev`/`ci`/`prod`).
  Secrets use the `DBT_ENV_SECRET_` prefix so dbt masks them and forbids them outside
  `profiles.yml`.

### 4. Schema routing is the whole game — own it in a macro
Override `generate_schema_name` (and `generate_database_name`) in a version-controlled
macro keyed on a `deployment_environment` var, not on per-developer `profiles.yml`
tweaks or platform UI settings.

- **`development`** → everything lands in ONE per-user schema, `{default_schema}_{user}`
  (e.g. `dbt_randy_pitcher`). Per-layer schema configs are ignored so a developer's whole
  build stays in their personal sandbox.
- **`ci_testing`** → `default_schema` is used UNMODIFIED. CI injects a build-scoped name
  like `dbt_{project}_pr{pr_number}_build{build_number}`.
- **`production`** → `default_schema` only when a model declares no custom schema;
  otherwise the model's `+schema` is used UNMODIFIED, so prod spreads across
  purpose-built namespaces.

- **Why:** routing in code means it's reviewable, testable, and identical for everyone.
- **In this repo:** `macros/config/generate_schema_name.sql` + `generate_database_name.sql`.

### 5. Per-developer dev sandboxes — never overwrite a teammate
In development, the schema is named after the developer, so two people building the same
model never collide.

- **Parse-time caveat (hard-won):** dbt resolves schema names at PARSE time, when there is
  no warehouse connection (`execute == false`). You therefore **cannot** call
  `select current_user()` in the macro — resolve the username from the environment
  (`DBT_DEV_USER` → OS `USER` → fallback) instead.
- **In this repo:** `macros/config/get_clean_username.sql`.

### 6. Every PR gets a disposable, fully-isolated CI schema
CI sets `default_schema` to a name carrying both the PR number and the build number, so a
re-run after a fix builds completely separately from the previous attempt, and teardown is
a single `drop schema cascade`.

- **In this repo:** `ci/github-actions-dbt-ci.yml.example` + the `drop_schema` run-operation.

### 7. Least privilege, per environment
Dev = SSO U2M as the human (no stored secret). CI/prod = dedicated M2M service principals,
credentials only ever in the runtime's secret store. No shared "god" token.

### 8. Raw/source data is read-only and shared across environments
Every environment reads the SAME upstream sources; only the outputs are namespaced. Dev
therefore faithfully replicates what prod will do.

- **In this repo:** `system.billing.*` sources, declared read-only; identical on every
  workspace and cloud.

### 9. Layered structure: staging → modeled → marts
`staging` cleans/renames (views), `modeled`/intermediate holds business logic (here, the
incremental priced fact), `marts` are consumption-ready aggregates (tables). Materialization
defaults are set per layer, not per model.

### 10. Test data AND transformations — and test for *correctness*, not just success
Beyond `not_null`/`unique`/`accepted_values`, write tests that would catch a "green build
that's silently wrong." A passing `dbt build` with all-NULL costs is the failure mode to
fear.

- **In this repo:** singular tests `assert_recent_cost_is_positive` and
  `assert_dbu_usage_is_priced`, plus `dbt_expectations` range/row-count checks.

### 11. Automate warehouse hygiene as run-operations
Dropping stale CI schemas, empty schemas, old relations — make these `dbt run-operation`
macros, not manual cleanup.

- **In this repo:** `macros/operations/drop_schema.sql`.

### 12. Version-control everything; let dbt push docs + lineage into Unity Catalog
All SQL in git; `persist_docs` pushes model/column descriptions into UC so the catalog is
self-documenting. (Grants-as-code via `+grants`/post-hooks is a natural next addition.)

---

## Provenance & status

"The One True Way" is the author's own term for this framework. It is used consistently in
internal Databricks field work — where it is framed as a **healthy Databricks deployment
lifecycle** built on environment isolation and management (the upgrade over the common
"simulate dev with folder isolation in the workspace" workaround). It is **not yet published**;
turning it into public writing + training is an active effort.

The *philosophy* is also visible in the public corpus — e.g. a dbt Labs "Overriding Schema
Generation" session and the `databricks/dbt-databricks#1144` issue.

This draft is synthesized alongside the reference implementation in this folder. Treat the
code as the source of truth and this document as the explanation, until both are finalized
together for publishing. (Internal source citations are tracked separately, outside this
public repo.)
