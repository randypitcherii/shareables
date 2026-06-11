# GitHub Actions self-hosted runner inside a Databricks App — spike

Spike for [#36](https://github.com/randypitcherii/shareables/issues/36): can a Databricks
App host a self-hosted GitHub Actions runner, so CI for `databricks/demos/dbt/` can run
**inside the workspace** using the app's service principal — no workspace credentials in
GitHub secrets, no external CI compute?

**Verdict: yes, it works end-to-end.** A workflow job executed on the runner and
`databricks current-user me` authenticated as the app's service principal with zero
GitHub-side secrets. See [go/no-go](#gono-go-recommendation) for the conditions.

## Probe results

| # | Probe | Result |
|---|-------|--------|
| 1 | Can the container execute the runner? | ✅ Ubuntu 22.04, x86_64, glibc 2.35 — the runner binary runs fine as the non-root `app` user (uid 1000). 4 vCPU / 15 GB RAM / ~190 GB disk observed. One gap: the image has **no libicu**, which `config.sh` hard-requires; fixed in user space (no root needed) by extracting `libicu70` from the Ubuntu archive and pointing `LD_LIBRARY_PATH` at it — `config.sh` greps `LD_LIBRARY_PATH` dirs for libicu, so the check passes and .NET gets real ICU. |
| 2 | Outbound networking | ✅ Egress to `github.com` over HTTPS is open. The 216 MB runner tarball downloaded in **1.3 s**; the runner's HTTPS long-poll connects instantly and jobs are picked up within seconds. `archive.ubuntu.com` (plain HTTP) is also reachable. |
| 3 | Auth | ✅ Registration token minted locally with `gh api .../actions/runners/registration-token` and pasted in (fine for the spike). Job steps inherit the container env, including `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`, so the Databricks CLI authenticates as the app SP from inside a job step with no setup at all. |
| 4 | Ephemerality | ✅ `--ephemeral` + a supervisor loop works: the runner deregisters after each job and the supervisor re-registers it. A registration token stays valid ~1 hour and is reusable, so one pasted token keeps the loop alive for that window. App restarts/redeploys wipe `/home/app` (runner + ICU re-download on next start — seconds, not minutes) and kill the supervisor; true self-healing across restarts needs a stored GitHub credential (see hardening). |

Proof run: [workflow run 27370283658](https://github.com/randypitcherii/shareables/actions/runs/27370283658)
— `runs-on: self-hosted`, echo + `databricks current-user me` returning the app SP
(`app-wplr48 randy-pitcher-gha-runner`).

## How it works

- The app is the [randy_apps_starter](../randy_apps_starter/) bash-over-REST template
  (Lakebase stripped — a runner needs no Postgres). The shell endpoints
  (`/api/v1/shell/run|stream|complete`) were the probe tool: every container experiment
  above was run via REST against the deployed app, no redeploys.
- `scripts/runner_supervisor.sh` does the runner lifecycle: download runner tarball +
  libicu if missing → `config.sh --ephemeral --unattended` → `run.sh` → loop.
- The FastAPI app manages the supervisor:
  - `GET /api/v1/runner/status` — supervisor/listener process state + log tail
  - `POST /api/v1/runner/start` — body `{"registration_token": "..."}` (plus optional
    `repo_url`, `runner_name`, `labels`); spawns the supervisor detached
  - `POST /api/v1/runner/stop` — kills supervisor + listener (runner falls offline and,
    being ephemeral, gets reaped by GitHub)
- The proof workflow is
  [`.github/workflows/self-hosted-runner-spike.yml`](../../../.github/workflows/self-hosted-runner-spike.yml)
  — `workflow_dispatch` + `push` to the spike branch only. **Deliberately no
  `pull_request` trigger** (public repo + self-hosted runner, see caveats).

## Reproduce

```bash
# 1. deploy + start the app (dev target)
make deploy-dev

# 2. mint a registration token (requires repo admin)
REG_TOKEN=$(gh api -X POST repos/randypitcherii/shareables/actions/runners/registration-token --jq .token)

# 3. start the runner supervisor inside the app
TOKEN=$(databricks auth token --output json | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')
curl -X POST "$APP_URL/api/v1/runner/start" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"registration_token\": \"$REG_TOKEN\"}"

# 4. confirm it's online, then trigger the proof workflow
gh api repos/randypitcherii/shareables/actions/runners --jq '.runners[].status'
gh workflow run self-hosted-runner-spike --ref <branch-with-the-workflow>
```

`scripts/app-shell.sh` runs ad-hoc commands inside the container for debugging:
`APP_URL=https://... ./scripts/app-shell.sh 'tail -20 /tmp/gha_runner_supervisor.log'`.

## Limitations

- **No Docker.** Container actions, `services:`, and docker-based steps won't work.
  Shell steps (and node-based actions like `actions/checkout`) are fine — dbt CI is
  pip/uv + CLI, so it fits.
- **Restarts are not self-healing yet.** The supervisor dies with the app container and
  the pasted registration token expires after ~1 hour. Each app restart/redeploy needs a
  fresh `POST /runner/start`. Fixable (see hardening), deliberately out of spike scope.
- **One runner = serial CI.** One job at a time. Fine for this repo's volume; more
  concurrency means more app replicas or multiple registered runners per container.
- **Always-on compute.** The app runs 24/7 whether or not CI jobs arrive. That's the
  trade for instant pickup; the dbt CI alternative (hosted runners + secrets) costs per
  minute instead.

## Security caveats — read before pointing real CI at this

- **Self-hosted runners on a public repo are explicitly discouraged by GitHub.** Anyone
  who can get a workflow to run on this runner executes arbitrary code inside the
  Databricks workspace network *as the app's service principal*.
- **Fork PRs MUST NOT be allowed to reach it.** Concretely:
  - Never add a `pull_request`-triggered workflow with `runs-on: self-hosted` to this
    repo. The spike workflow uses `workflow_dispatch` + same-repo `push` only, plus an
    `if: github.repository == ... && github.actor == 'randypitcherii'` guard.
  - Repo Actions settings should require approval for **all** outside collaborators
    (Settings → Actions → Fork pull request workflows).
- **Job steps can read the app SP's client secret** from the environment. Any workflow
  that lands on this runner owns the SP. Scope the SP to the minimum needed (the dbt CI
  SP needs little beyond its dev schema + warehouse), and prefer a dedicated app/SP for
  CI rather than reusing an app that has broader grants.
- The `/runner/start` + shell endpoints are behind Databricks app auth (workspace users
  with CAN_USE on the app) — do not grant the app to `users` broadly.

## Go/no-go recommendation

**Conditional GO.** The mechanics are proven and pleasantly boring: the container runs
the stock runner binary, egress just works, and in-workspace auth from job steps is
free. For enabling the dbt CI workflow (`databricks/demos/dbt/ci/`), do it only with all
of the following:

1. **Trigger hygiene**: `workflow_dispatch` and/or `push` to trusted branches only — no
   `pull_request` trigger on this public repo. (If PR-triggered CI is the actual goal,
   move the dbt demo CI to a private mirror or accept hosted runners with secrets.)
2. **Token automation**: store a fine-grained PAT (or GitHub App key) with
   `administration:write` on the repo in a Databricks secret, have the app mint
   registration tokens itself at boot — that makes restarts fully self-healing and
   removes the manual paste.
3. **Dedicated least-privilege SP**: a CI-only app whose SP has exactly the dbt dev
   schema + warehouse grants, nothing else.

Without 1–3, keep this as a demo. The pattern's sweet spot is private repos / internal
projects, where it deletes both the secrets-in-GitHub problem and external CI spend in
one move.
