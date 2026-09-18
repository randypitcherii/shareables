.DEFAULT_GOAL := help
.PHONY: help install lint test test-infrastructure auth-check \
	tf-init tf-plan tf-apply tf-destroy \
	setup-uc teardown-uc verify \
	run-01 run-02 run-03 run-04 run-05 run-06 run-07 run-08 run-09 run-10 run-core run-all run-reader-side

# Config comes from $(APP_ENV).env (copy template.env -> dev.env). Real env vars win.
APP_ENV ?= dev
-include $(APP_ENV).env
export

DATABRICKS_CONFIG_PROFILE ?= DEFAULT
AWS_PROFILE ?= your-aws-profile-name
TF := terraform -chdir=terraform
M  := PYTHONPATH=scripts:scripts/matrix uv run python scripts/matrix

help: ## Show this help
	@echo "glue-iceberg-list-type-uc-federation — bounds of UC Glue federation for Iceberg list/array columns"
	@echo "==========================================================================================="
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-20s %s\n", $$1, $$2}'
	@echo ""
	@echo "Config: APP_ENV=$(APP_ENV) (loads $(APP_ENV).env; see template.env)"
	@echo "  DATABRICKS_CONFIG_PROFILE=$(DATABRICKS_CONFIG_PROFILE)  AWS_PROFILE=$(AWS_PROFILE)"
	@echo "  AWS_REGION=$(AWS_REGION)  GLUE_DATABASE=$(GLUE_DATABASE)  UC_FOREIGN_CATALOG=$(UC_FOREIGN_CATALOG)"
	@echo ""
	@echo "Order: tf-apply -> (paste env snippet) -> setup-uc -> verify -> run-core -> run-all -> teardown-uc -> tf-destroy"

install: ## uv sync dependencies
	uv sync

lint: ## ruff over scripts + tests
	uv run ruff check scripts/ tests/

test: ## unit tests (no infrastructure)
	uv run pytest tests/ -m "not infrastructure"

test-infrastructure: auth-check ## tests requiring live Glue + Databricks
	uv run pytest tests/ -m infrastructure

auth-check: ## verify Databricks + AWS auth surfaces
	@echo "--- Databricks ($(DATABRICKS_CONFIG_PROFILE)) ---"
	@databricks current-user me --profile $(DATABRICKS_CONFIG_PROFILE) > /dev/null && echo "Databricks: OK" \
		|| echo "Databricks: FAILED — run: databricks auth login --profile $(DATABRICKS_CONFIG_PROFILE)"
	@echo "--- AWS ($(AWS_PROFILE)) ---"
	@aws sts get-caller-identity --profile $(AWS_PROFILE) > /dev/null && echo "AWS: OK" \
		|| echo "AWS: FAILED — run: aws sso login --profile $(AWS_PROFILE)"

tf-init: ## terraform init
	$(TF) init

tf-plan: ## terraform plan
	$(TF) plan

tf-apply: ## create S3 bucket, Glue database, IAM role, Athena workgroup
	$(TF) apply
	@echo ""
	@echo "Paste into $(APP_ENV).env:"
	@$(TF) output -raw env_snippet

tf-destroy: ## destroy ALL AWS-side infrastructure
	$(TF) destroy

setup-uc: auth-check ## create UC service/storage credential, external location, Glue connection, foreign catalog
	uv run python scripts/00_setup_uc.py

teardown-uc: ## drop the UC objects + all experiment Glue tables (reverse of setup-uc)
	uv run python scripts/01_teardown_uc.py

verify: auth-check ## prove every auth surface with one real call each
	uv run python scripts/verify.py

run-01: ## baseline: primitives via Glue REST endpoint readable in UC?
	$(M)/01_baseline_primitives_rest.py
run-02: ## the reported failure: list<bigint> via Glue REST endpoint
	$(M)/02_list_primitive_rest.py
run-03: ## same shape via native GlueCatalog (array<> in SD?)
	$(M)/03_list_primitive_glue_native.py
run-04: ## sweep every type shape x writer path
	$(M)/04_type_shape_sweep.py
run-05: ## which SQL surfaces fail, and with what class
	$(M)/05_failure_surfaces.py
run-06: ## SD patch list->array: works? survives next commit?
	$(M)/06_sd_patch_durability.py
run-07: ## Athena reads the same table? (is this UC-specific)
	$(M)/07_athena_cross_reader.py
run-08: ## any UC-side override that sidesteps SD parsing
	$(M)/08_uc_side_overrides.py

run-core: verify run-01 run-02 run-03 ## the three rows that answer the burning question

run-09: ## reader-side mirror: shadow Glue table via register_table (stale? survives commits?)
	$(M)/09_shadow_register_table.py
run-10: ## federate to the Glue Iceberg REST endpoint instead (ICEBERG_REST connection)
	$(M)/10_iceberg_rest_federation.py

run-reader-side: verify run-09 run-10 ## the two rows that answer "the writer is fixed — now what?"

run-all: verify run-01 run-02 run-03 run-04 run-05 run-06 run-07 run-08 run-09 run-10 ## verify, then every matrix row
