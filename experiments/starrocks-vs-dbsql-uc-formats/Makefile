.DEFAULT_GOAL := help
.PHONY: help install lint test test-infrastructure auth-check \
	tf-init tf-plan tf-apply tf-destroy starrocks-health \
	setup-uc verify run-battery run-interop run-all teardown-check

# Config comes from $(APP_ENV).env (copy template.env -> dev.env). Real env vars win.
APP_ENV ?= dev
-include $(APP_ENV).env
export

DATABRICKS_CONFIG_PROFILE ?= DEFAULT
AWS_PROFILE ?= your-aws-profile-name
TF := terraform -chdir=terraform

help: ## Show this help
	@echo "starrocks-vs-dbsql-uc-formats — evaluation commands"
	@echo "===================================================="
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-22s %s\n", $$1, $$2}'
	@echo ""
	@echo "Config: APP_ENV=$(APP_ENV) (loads $(APP_ENV).env; see template.env)"
	@echo "  DATABRICKS_CONFIG_PROFILE=$(DATABRICKS_CONFIG_PROFILE)  AWS_PROFILE=$(AWS_PROFILE)"

install: ## uv sync dependencies
	uv sync

lint: ## ruff over scripts + tests
	uv run ruff check scripts/ tests/

test: ## unit tests (no infrastructure)
	uv run pytest tests/ -m "not infrastructure"

test-infrastructure: auth-check ## tests requiring live StarRocks + Databricks
	uv run pytest tests/ -m infrastructure

auth-check: ## verify Databricks + AWS auth surfaces
	@echo "--- Databricks ($(DATABRICKS_CONFIG_PROFILE)) ---"
	@databricks current-user me --profile $(DATABRICKS_CONFIG_PROFILE) > /dev/null && echo "Databricks: OK" \
		|| echo "Databricks: FAILED — run: databricks auth login"
	@echo "--- AWS ($(AWS_PROFILE)) ---"
	@aws sts get-caller-identity --profile $(AWS_PROFILE) > /dev/null && echo "AWS: OK" \
		|| echo "AWS: FAILED — run: aws sso login --profile $(AWS_PROFILE)"

tf-init: ## terraform init
	$(TF) init

tf-plan: ## terraform plan
	$(TF) plan

tf-apply: ## create the StarRocks VM
	$(TF) apply
	@echo ""
	@echo "Update your $(APP_ENV).env from these outputs:"
	@$(TF) output

tf-destroy: ## destroy ALL evaluation infrastructure
	$(TF) destroy

starrocks-health: ## check the FE answers on the MySQL port
	@uv run python -c "import os,pymysql; c=pymysql.connect(host=os.environ['STARROCKS_HOST'],port=int(os.environ.get('STARROCKS_PORT','9030')),user=os.environ.get('STARROCKS_USER','root'),password=os.environ.get('STARROCKS_PASSWORD','')); cur=c.cursor(); cur.execute('SELECT current_version()'); print('StarRocks up:', cur.fetchone()[0])"

setup-uc: ## create the UC schema + EXTERNAL USE SCHEMA grant
	uv run python scripts/00_setup_uc.py

verify: ## prove all auth + connectivity surfaces with real calls
	uv run python scripts/verify.py

run-battery: ## run all five (engine, format) CRUD/aggregation batteries
	uv run python scripts/battery/01_starrocks_native.py
	uv run python scripts/battery/02_starrocks_uc_iceberg.py
	uv run python scripts/battery/03_starrocks_uc_delta.py
	uv run python scripts/battery/04_dbsql_delta.py
	uv run python scripts/battery/05_dbsql_iceberg.py

run-interop: ## run the cross-engine read/write interop matrix
	uv run python scripts/interop/06_interop_read.py
	uv run python scripts/interop/07_interop_write.py

run-all: verify run-battery run-interop ## verify, then the full evaluation

teardown-check: ## prove no evaluation EC2 instances remain
	aws ec2 describe-instances --profile $(AWS_PROFILE) --region $${AWS_REGION:-us-east-1} \
		--filters "Name=tag:project,Values=starrocks-vs-dbsql-uc-formats" \
		--query 'Reservations[].Instances[].{id:InstanceId,state:State.Name}' --output table
