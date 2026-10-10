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

# The dev bundle deploys into the same catalog dbt dev builds use:
# DBT_DEFAULT_CATALOG from the environment or .env (dbt reads .env itself; the
# databricks CLI does not, so make passes it through), else the rpw_dev default.
DEV_CATALOG := $(or $(DBT_DEFAULT_CATALOG),$(shell sed -n 's/^DBT_DEFAULT_CATALOG=//p' .env 2>/dev/null | tail -1),rpw_dev)
GIT_SHA := $(shell git rev-parse HEAD)

.PHONY: venv deps build run test pytest patterns patterns-seeds ai-ml-inference ai-ml-inference-endpoint ai-gateway ai-gateway-eval ai-gateway-dashboard s3-storage s3-storage-dashboard deploy deploy-dev deploy-prod deploy-prod-local

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

# ---- migration pattern examples (models/patterns/README.md) ---------------

# build + test every pattern example (synthetic data, never on the daily schedule)
patterns: deps
	uv run dbt build -s tag:patterns

# regenerate the reference seeds (IANA timezones, ISO 3166-1 region codes)
patterns-seeds:
	uv run scripts/generate_pattern_seeds.py

# ---- batch inference from a UC-logged MLflow model (models/ai_ml_inference/) --

# build + test both inference paths. The ai_query path runs only when
# DBT_AI_ML_INFERENCE_SERVING_ENDPOINT is set (in .env or the environment).
ai-ml-inference: deps
	uv run dbt build -s tag:ai_ml_inference

# create (or re-point) YOUR serving endpoint from serving_endpoint.json and wait
# until it is READY. One endpoint per developer: ai-ml-inference-<user>.
# Uses the databricks CLI's normal auth (DATABRICKS_CONFIG_PROFILE or DEFAULT).
AI_ML_INFERENCE_ENDPOINT ?= ai-ml-inference-$(shell printf '%s' "$${DBT_DEV_USER:-$$USER}" | cut -d@ -f1 | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9\n' '-')
AI_ML_INFERENCE_SPEC := models/ai_ml_inference/serving_endpoint.json
ai-ml-inference-endpoint:
	@want=$$(jq -c '[.config.served_entities[] | {entity_name, entity_version}]' $(AI_ML_INFERENCE_SPEC)); \
	if current=$$(databricks serving-endpoints get $(AI_ML_INFERENCE_ENDPOINT) -o json 2>/dev/null); then \
		have=$$(echo "$$current" | jq -c '[(.pending_config // .config).served_entities[] | {entity_name, entity_version}]'); \
		if [ "$$have" = "$$want" ]; then \
			echo "$(AI_ML_INFERENCE_ENDPOINT) already serves $$want"; \
		else \
			echo "re-pointing $(AI_ML_INFERENCE_ENDPOINT): $$have -> $$want"; \
			jq '.config' $(AI_ML_INFERENCE_SPEC) | databricks serving-endpoints update-config $(AI_ML_INFERENCE_ENDPOINT) --no-wait --json @/dev/stdin >/dev/null; \
		fi; \
	else \
		echo "creating $(AI_ML_INFERENCE_ENDPOINT) -> $$want"; \
		jq --arg n $(AI_ML_INFERENCE_ENDPOINT) 'del(.comment) | .name = $$n' $(AI_ML_INFERENCE_SPEC) | databricks serving-endpoints create --no-wait --json @/dev/stdin >/dev/null; \
	fi
	@echo "waiting for READY (a first deploy takes several minutes) ..."
	@while :; do \
		state=$$(databricks serving-endpoints get $(AI_ML_INFERENCE_ENDPOINT) -o json | jq -r '.state.ready + "/" + .state.config_update'); \
		case "$$state" in \
			READY/NOT_UPDATING) break ;; \
			*UPDATE_FAILED*) echo "deploy failed: see the endpoint's events in the Serving UI"; exit 1 ;; \
		esac; \
		echo "  $$state"; sleep 30; \
	done
	@echo "ready. add to .env:  DBT_AI_ML_INFERENCE_SERVING_ENDPOINT=$(AI_ML_INFERENCE_ENDPOINT)"

# ---- AI Gateway improvement loop (models/ai_gateway_improvement/README.md) ---

# build + test the loop into your dev schema (what dbt Hourly runs in prod).
# Defaults are production-sized; for a quick first look try:
#   DBT_AI_GATEWAY_HISTORY_DAYS=3 DBT_AI_GATEWAY_MAX_SESSIONS_PER_RUN=25 make ai-gateway
ai-gateway: deps
	uv run dbt build -s tag:ai_gateway_improvement

# score the hand-labelled golden set with the live rubrics; fails below the
# accuracy floors. Run it after every rubric edit.
ai-gateway-eval: deps
	uv run dbt build -s tag:ai_gateway_eval

# regenerate the dashboard JSON from scripts/build_ai_gateway_dashboard.py
ai-gateway-dashboard:
	uv run --no-project python scripts/build_ai_gateway_dashboard.py

# ---- S3 storage analytics (README: "S3 storage analytics") ------------------

# build + test the S3 Inventory pipeline, metric views included, into your dev schema
s3-storage: deps
	uv run dbt build -s tag:s3_inventory

# regenerate the dashboard JSON from scripts/build_s3_storage_dashboard.py
s3-storage-dashboard:
	uv run --no-project python scripts/build_s3_storage_dashboard.py

# ---- DABs deploys -----------------------------------------------------------

# `make deploy` is the everyday deploy: your isolated dev copy of the bundle
deploy: deploy-dev

deploy-dev:
	databricks bundle deploy -t dev --var="deployed_at=$(DEPLOY_DATE)" --var="dev_catalog=$(DEV_CATALOG)"

deploy-prod:
	@echo "Triggering dbt CD at $(GIT_SHA) (commit must be pushed)"
	databricks jobs run-now $$(databricks jobs list -o json --name "dbt CD" | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["job_id"])') \
		--json '{"job_parameters": {"git_ref": "$(GIT_SHA)"}}' --no-wait

deploy-prod-local:
	@echo "WARNING: local prod deploy flips job ownership to you and breaks CD deploys"
	@echo "         until an admin transfers job ownership back to the production SP."
	databricks bundle deploy -t prod --var="deployed_at=$(DEPLOY_DATE)"
