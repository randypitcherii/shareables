.PHONY: dev test verify deploy-dev deploy-prod list

list:
	@echo "Available targets:"
	@echo "  dev         - Start local dev server with hot-reload"
	@echo "  test        - Run pytest"
	@echo "  verify      - Run tests + validate DAB bundles"
	@echo "  deploy-dev  - Deploy to dev target"
	@echo "  deploy-prod - Deploy to prod target"
	@echo "  list        - Show this help"

dev:
	uv sync
	@# Kill any existing process on port 8000
	-lsof -ti:8000 | xargs kill 2>/dev/null || true
	uv run uvicorn app:app --reload --host 127.0.0.1 --port 8000

test:
	uv run pytest tests/ -v

verify: test
	databricks bundle validate -t dev

deploy-dev: verify
	databricks bundle deploy -t dev
	$(eval SRC := $(shell databricks bundle validate -t dev --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('workspace',{}).get('file_path',''))"))
	databricks apps deploy better-agent-gateway-dev --source-code-path $(SRC)
	@echo "Deployed and restarted dev app."

deploy-prod: verify
	databricks bundle deploy -t prod
	$(eval SRC := $(shell databricks bundle validate -t prod --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('workspace',{}).get('file_path',''))"))
	databricks apps deploy better-agent-gateway-prod --source-code-path $(SRC)
	@echo "Deployed and restarted prod app."
