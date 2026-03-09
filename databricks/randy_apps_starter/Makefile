UV ?= uv
LOCAL_REVIEW_PORT := 8000
BRANCH_SLUG ?= $(shell ./scripts/git-branch-slug.sh)
WORKSPACE_USER ?= $(shell databricks current-user me --output json | python3 -c 'import sys,json; print(json.load(sys.stdin)["userName"])')
DEV_WORKSPACE_SOURCE_PATH ?= /Workspace/Users/$(WORKSPACE_USER)/.bundle/randy_apps_starter/dev/files
DEV_APP_BASE ?= apps-sbx
DEV_PROJECT_BASE ?= randy-apps-starter
DEV_APP_NAME ?= $(shell python3 ./dev_resource_name.py "$(DEV_APP_BASE)" "" 30)
DEV_LAKEBASE_PROJECT_ID ?= $(shell python3 ./dev_resource_name.py "$(DEV_PROJECT_BASE)")

.PHONY: dev test verify deploy-dev deploy-prod

dev:
	$(UV) sync
	@pids=$$(lsof -ti tcp:$(LOCAL_REVIEW_PORT) -sTCP:LISTEN || true); \
	if [ -n "$$pids" ]; then \
		echo "Stopping existing local reviewer on port $(LOCAL_REVIEW_PORT): $$pids"; \
		kill $$pids; \
		sleep 1; \
	fi
	$(UV) run uvicorn app:app --reload --host 127.0.0.1 --port $(LOCAL_REVIEW_PORT)

test:
	$(UV) sync --group dev
	$(UV) run pytest -q

verify: test
	databricks bundle validate -t dev \
		--var "git_branch_slug=$(BRANCH_SLUG)" \
		--var "app_name=$(DEV_APP_NAME)" \
		--var "lakebase_project_id=$(DEV_LAKEBASE_PROJECT_ID)"
	databricks bundle validate -t prod

deploy-dev:
	databricks bundle deploy -t dev \
		--var "git_branch_slug=$(BRANCH_SLUG)" \
		--var "app_name=$(DEV_APP_NAME)" \
		--var "lakebase_project_id=$(DEV_LAKEBASE_PROJECT_ID)"
	databricks apps deploy $(DEV_APP_NAME) --source-code-path "$(DEV_WORKSPACE_SOURCE_PATH)"

deploy-prod:
	databricks bundle deploy -t prod
	databricks apps deploy randy-apps-starter --source-code-path .
