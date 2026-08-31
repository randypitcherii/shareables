# Local workflow + deploy wrappers. Three rules encoded here:
#
#   * dbt NEVER runs from a global install (THE_ONE_TRUE_WAY #16). Every dbt
#     command goes through `uv run` inside the project venv, and every dbt
#     target depends on `deps` -> `venv`, so the venv always matches
#     pyproject.toml (the single source of truth for the dbt version) and dbt
#     packages are always installed. Both are near-instant no-ops when current.
#
#   * `deployed_at` tag stamping (THE_ONE_TRUE_WAY #14) -- DABs has no date
#     function, so the shell provides it.
#
#   * PROD DEPLOYS GO THROUGH THE CD JOB, not a local `bundle deploy`. The
#     terraform provider sets each job's owner to whoever runs the deploy, and
#     only workspace admins may CHANGE a job owner -- so one local human deploy
#     flips ownership to the human and every subsequent CD (service principal)
#     deploy 403s until an admin transfers ownership back. `deploy-prod`
#     therefore triggers the `dbt CD` job at your current HEAD (which must be
#     pushed). `deploy-prod-local` exists for bootstrap (the CD job doesn't
#     exist yet) and emergencies -- after using it, re-transfer job ownership
#     to the production SP or CD stays broken.

DEPLOY_DATE := $(shell date +%Y-%m-%d)
GIT_SHA := $(shell git rev-parse HEAD)

.PHONY: venv deps build run test pytest deploy deploy-dev deploy-prod deploy-prod-local

# ---- uv venv management ----------------------------------------------------

# create the venv if missing and sync it to pyproject.toml
venv:
	uv sync

# install dbt packages (dbt_utils, dbt_expectations, ...) from packages.yml
deps: venv
	uv run dbt deps

# ---- core dbt commands (dev target by default; .env is auto-loaded) --------

build: deps
	uv run dbt build

run: deps
	uv run dbt run

test: deps
	uv run dbt test

# python tests for the deployment config itself (catalog defaults, etc.)
pytest: venv
	uv run --with pytest pytest tests -q

# ---- DABs deploys -----------------------------------------------------------

# `make deploy` is the everyday deploy: your isolated dev copy of the bundle
deploy: deploy-dev

deploy-dev:
	databricks bundle deploy -t dev --var="deployed_at=$(DEPLOY_DATE)"

deploy-prod:
	@echo "Triggering dbt CD at $(GIT_SHA) (commit must be pushed)"
	databricks jobs run-now $$(databricks jobs list -o json --name "dbt CD" | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["job_id"])') \
		--json '{"job_parameters": {"git_ref": "$(GIT_SHA)"}}' --no-wait

deploy-prod-local:
	@echo "WARNING: local prod deploy flips job ownership to you and breaks CD deploys"
	@echo "         until an admin transfers job ownership back to the production SP."
	databricks bundle deploy -t prod --var="deployed_at=$(DEPLOY_DATE)"
