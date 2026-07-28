.PHONY: help install lint test test-infrastructure auth-check \
	tf-init tf-plan tf-apply tf-destroy pulsar-health \
	produce run-path-a run-path-b run-path-c verify

# Config comes from $(APP_ENV).env (copy template.env -> dev.env). Real env vars win.
APP_ENV ?= dev
-include $(APP_ENV).env
export

DATABRICKS_CONFIG_PROFILE ?= DEFAULT
AWS_PROFILE ?= your-aws-profile-name
TF := terraform -chdir=terraform
BUNDLE_VARS = --var="pulsar_service_url=$(PULSAR_SERVICE_URL)" \
	--var="kafka_bootstrap=$(KAFKA_BOOTSTRAP)" \
	--var="uc_catalog=$(UC_CATALOG)" \
	--var="uc_schema=$(UC_SCHEMA)"

help:
	@echo "pulsar-to-uc-managed-tables — evaluation commands"
	@echo "=================================================="
	@echo ""
	@echo "Setup:"
	@echo "  make install             - uv sync dependencies"
	@echo "  make auth-check          - verify Databricks + AWS auth"
	@echo "  make tf-apply            - create the Pulsar VM (also: tf-init/tf-plan/tf-destroy)"
	@echo "  make pulsar-health       - check the broker is up"
	@echo ""
	@echo "Evaluation:"
	@echo "  make produce             - publish synthetic events to the topic"
	@echo "  make run-path-a          - KoP kafka source -> managed Delta (Databricks job, GA)"
	@echo "  make run-path-b          - native pulsar connector -> managed Delta (Databricks job, preview)"
	@echo "  make run-path-c          - external PyIceberg via UC Iceberg REST -> managed Iceberg"
	@echo "  make verify              - check row counts + MANAGED status; writes results/"
	@echo ""
	@echo "Quality:"
	@echo "  make test                - unit tests (no infrastructure)"
	@echo "  make test-infrastructure - tests requiring live Pulsar + Databricks"
	@echo "  make lint                - ruff"
	@echo ""
	@echo "Config: APP_ENV=$(APP_ENV) (loads $(APP_ENV).env; see template.env)"

install:
	uv sync

lint:
	uv run ruff check scripts/ tests/ databricks/src/

test:
	uv run pytest tests/ -m "not infrastructure"

test-infrastructure:
	uv run pytest tests/ -m infrastructure

auth-check:
	@echo "--- Databricks ($(DATABRICKS_CONFIG_PROFILE)) ---"
	@databricks current-user me --profile $(DATABRICKS_CONFIG_PROFILE) > /dev/null && echo "Databricks: OK" \
		|| echo "Databricks: FAILED — run: databricks auth login"
	@echo "--- AWS ($(AWS_PROFILE)) ---"
	@aws sts get-caller-identity --profile $(AWS_PROFILE) > /dev/null && echo "AWS: OK" \
		|| echo "AWS: FAILED — run: aws sso login --profile $(AWS_PROFILE)"

tf-init:
	$(TF) init

tf-plan:
	$(TF) plan

tf-apply:
	$(TF) apply
	@echo ""
	@echo "Update your $(APP_ENV).env from these outputs:"
	@$(TF) output

tf-destroy:
	$(TF) destroy

pulsar-health:
	curl -sf $(PULSAR_ADMIN_URL)/admin/v2/brokers/health && echo " — broker healthy"

produce:
	uv run python scripts/00_generate_events.py

run-path-a:
	cd databricks && databricks bundle deploy --profile $(DATABRICKS_CONFIG_PROFILE) --target dev $(BUNDLE_VARS)
	cd databricks && databricks bundle run path_a_kop_kafka --profile $(DATABRICKS_CONFIG_PROFILE) --target dev $(BUNDLE_VARS)

run-path-b:
	cd databricks && databricks bundle deploy --profile $(DATABRICKS_CONFIG_PROFILE) --target dev $(BUNDLE_VARS)
	cd databricks && databricks bundle run path_b_native_pulsar --profile $(DATABRICKS_CONFIG_PROFILE) --target dev $(BUNDLE_VARS)

run-path-c:
	uv run python scripts/03_path_c_pyiceberg_rest.py

verify:
	uv run python scripts/verify.py
