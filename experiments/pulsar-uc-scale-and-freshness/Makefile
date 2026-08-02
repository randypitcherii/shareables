.PHONY: help install lint test test-infrastructure auth-check \
	tf-init tf-plan tf-apply tf-destroy pulsar-health deploy-jobs \
	produce produce-remote produce-remote-timed \
	run-scale-a run-scale-b run-scale-c run-scale-c-filtered \
	run-ladder-a run-ladder-b run-ladder-c-nrt run-ladder-c-t60 \
	run-reads cost-model verify

# Config comes from $(APP_ENV).env (copy template.env -> dev.env). Real env vars win.
APP_ENV ?= dev
-include $(APP_ENV).env
export

DATABRICKS_CONFIG_PROFILE ?= DEFAULT
AWS_PROFILE ?= your-aws-profile-name
TF := terraform -chdir=terraform
SSH_OPTS := -i $(SSH_KEY) -o StrictHostKeyChecking=accept-new

help:
	@echo "pulsar-uc-scale-and-freshness — evaluation commands"
	@echo "===================================================="
	@echo ""
	@echo "Setup:"
	@echo "  make install              - uv sync dependencies"
	@echo "  make auth-check           - verify Databricks + AWS auth"
	@echo "  make tf-apply             - create the Pulsar VM (also: tf-init/tf-plan/tf-destroy)"
	@echo "  make pulsar-health        - check the broker is up"
	@echo "  make deploy-jobs          - deploy the DAB (paths A and B)"
	@echo ""
	@echo "Produce (RUN_ID=$(RUN_ID) EVENT_COUNT=$(EVENT_COUNT) EVENT_RATE_PER_SEC=$(EVENT_RATE_PER_SEC)):"
	@echo "  make produce              - produce from this machine (small runs only)"
	@echo "  make produce-remote       - produce ON the broker VM (scale runs; EVENT_COUNT bounded)"
	@echo "  make produce-remote-timed - produce ON the VM for DURATION_SEC at EVENT_RATE_PER_SEC"
	@echo ""
	@echo "Evaluation:"
	@echo "  make run-scale-a          - path A drains: plain + filtered + clustered (one job run)"
	@echo "  make run-scale-b          - path B drain: plain"
	@echo "  make run-scale-c          - path C drain: plain (external writer)"
	@echo "  make run-scale-c-filtered - path C drain with consumer-side filter"
	@echo "  make run-ladder-a         - path A freshness ladder: NRT, 1m, 5m (continuous producer!)"
	@echo "  make run-ladder-b         - path B freshness ladder: NRT, 1m"
	@echo "  make run-ladder-c-nrt     - path C window run, ~5s commits"
	@echo "  make run-ladder-c-t60     - path C window run, 60s commits"
	@echo "  make run-reads            - clustering read benchmark + timed OPTIMIZE FULL"
	@echo "  make cost-model           - cost/perf model from measured results"
	@echo "  make verify               - managed-ness, VARIANT, freshness, filter reduction -> results/"
	@echo ""
	@echo "Quality:"
	@echo "  make test                 - unit tests (no infrastructure)"
	@echo "  make test-infrastructure  - tests requiring live Pulsar + Databricks"
	@echo "  make lint                 - ruff"
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

deploy-jobs:
	cd databricks && databricks bundle deploy --profile $(DATABRICKS_CONFIG_PROFILE) --target dev

produce:
	uv run python scripts/00_generate_events.py

produce-remote:
	scp $(SSH_OPTS) scripts/00_generate_events.py ec2-user@$(SSH_HOST):/opt/pulsar/producer/gen.py
	ssh $(SSH_OPTS) ec2-user@$(SSH_HOST) "docker run --rm --network host \
		-v /opt/pulsar/producer:/work \
		-e PULSAR_SERVICE_URL=pulsar://localhost:6650 \
		-e PULSAR_TOPIC=$(PULSAR_TOPIC) \
		-e EVENT_COUNT=$(EVENT_COUNT) \
		-e EVENT_RATE_PER_SEC=$(EVENT_RATE_PER_SEC) \
		pulsar-producer python /work/gen.py"

produce-remote-timed:
	scp $(SSH_OPTS) scripts/00_generate_events.py ec2-user@$(SSH_HOST):/opt/pulsar/producer/gen.py
	ssh $(SSH_OPTS) ec2-user@$(SSH_HOST) "docker run --rm --network host \
		-v /opt/pulsar/producer:/work \
		-e PULSAR_SERVICE_URL=pulsar://localhost:6650 \
		-e PULSAR_TOPIC=$(PULSAR_TOPIC) \
		-e EVENT_COUNT=0 \
		-e DURATION_SEC=$(DURATION_SEC) \
		-e EVENT_RATE_PER_SEC=$(EVENT_RATE_PER_SEC) \
		pulsar-producer python /work/gen.py"

run-scale-a:
	uv run python scripts/01_run_databricks_cells.py scale-a

run-scale-b:
	uv run python scripts/01_run_databricks_cells.py scale-b

run-scale-c:
	uv run python scripts/03_path_c_writer.py --cell c_scale_plain --mode drain

run-scale-c-filtered:
	uv run python scripts/03_path_c_writer.py --cell c_scale_filtered --mode drain --filter on

run-ladder-a:
	uv run python scripts/01_run_databricks_cells.py ladder-a

run-ladder-b:
	uv run python scripts/01_run_databricks_cells.py ladder-b

run-ladder-c-nrt:
	uv run python scripts/03_path_c_writer.py --cell c_nrt --mode window --window-sec 480 --commit-interval 5

run-ladder-c-t60:
	uv run python scripts/03_path_c_writer.py --cell c_t60 --mode window --window-sec 600 --commit-interval 60

run-reads:
	uv run python scripts/04_read_queries.py

cost-model:
	uv run python scripts/05_cost_model.py

verify:
	uv run python scripts/verify.py
