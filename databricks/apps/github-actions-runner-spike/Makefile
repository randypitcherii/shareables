UV ?= uv
LOCAL_REVIEW_PORT := 8000
WORKSPACE_USER ?= $(shell databricks current-user me --output json | python3 -c 'import sys,json; print(json.load(sys.stdin)["userName"])')
DEV_WORKSPACE_SOURCE_PATH ?= /Workspace/Users/$(WORKSPACE_USER)/.bundle/github_actions_runner_spike/dev/files
DEV_APP_BASE ?= gha-runner
DEV_APP_NAME ?= $(shell python3 ./dev_resource_name.py "$(DEV_APP_BASE)" "" 30)

.PHONY: dev test verify deploy-dev

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
	databricks bundle validate -t dev --var "app_name=$(DEV_APP_NAME)"

deploy-dev:
	databricks bundle deploy -t dev --var "app_name=$(DEV_APP_NAME)"
	databricks apps deploy $(DEV_APP_NAME) --source-code-path "$(DEV_WORKSPACE_SOURCE_PATH)"
