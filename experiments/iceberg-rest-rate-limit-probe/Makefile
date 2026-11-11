.PHONY: help install lint test auth-check setup smoke probe probe-all probe-scope \
	analyze plot upload dashboard cleanup job-deploy probe-job job-destroy

# Config comes from $(APP_ENV).env (copy template.env -> dev.env). Real env vars win.
APP_ENV ?= dev
export

PYTHONUNBUFFERED := 1
ENDPOINT ?= config
WORKSPACES ?= a
PROBE_ARGS ?=
SP_SCOPE ?=
RUN := uv run python scripts
# Profiles come from $(APP_ENV).env via _common.load_config (env vars win).
PROFILES = $$(cd scripts && uv run python -c "from _common import load_config; c = load_config(); print(c.profile_a, c.profile_b)")

# Single-workspace endpoint sweep (each run is bounded; see PROBE_MAX_* in template.env).
SWEEP := config list_tables load_table_spread load_table_hot load_table_nocreds \
	legacy_config legacy_load_table
# Scope test: the same endpoints driven from two workspaces on one metastore at once.
SCOPE_SWEEP := config load_table_spread

help:
	@echo "iceberg-rest-rate-limit-probe: find where Iceberg REST 429s start"
	@echo "================================================================"
	@echo "Setup (transient sandbox workspaces ONLY):"
	@echo "  make install          uv sync"
	@echo "  make auth-check       verify the workspace profiles in $(APP_ENV).env"
	@echo "  make setup            schema, $(PROBE_TABLE_COUNT) Iceberg tables, UniForm table, volume, grant"
	@echo "  make smoke            PyIceberg sanity check (config, list, load, scan)"
	@echo ""
	@echo "Probe (live graph at http://127.0.0.1:8765/ while it runs):"
	@echo "  make probe ENDPOINT=load_table_spread [WORKSPACES=a,b] [PROBE_ARGS='--rates 10,20']"
	@echo "  make probe-all        sweep: $(SWEEP)"
	@echo "  make probe-scope      two-workspace sweep: $(SCOPE_SWEEP)"
	@echo "  make job-deploy       deploy the serverless probe job to each workspace (bundle)"
	@echo "  make probe-job ENDPOINT=load_table_spread [WORKSPACES=a,b] [SP_SCOPE=b=<secret-scope>]"
	@echo "                       run it in-workspace (SP_SCOPE: call as a service principal in b)"
	@echo ""
	@echo "Results:"
	@echo "  make analyze          data/ -> scrubbed results/ (summary.json, per_second/, samples_429.json)"
	@echo "  make plot             results/loadtable_scope.png (one workspace vs two)"
	@echo "  make upload           raw logs -> probe_calls Delta table + views"
	@echo "  make dashboard        deploy + publish the AI/BI dashboard"
	@echo "  make cleanup          drop dashboard + schema (tables, views, volume) + bundle jobs"
	@echo ""
	@echo "Quality: make test | make lint"

install:
	uv sync

lint:
	uv run ruff check scripts/ tests/ dashboard/

test:
	uv run pytest tests/ -m "not infrastructure"

auth-check:
	@for p in $(PROFILES); do \
		databricks current-user me --profile $$p > /dev/null && echo "$$p: OK" \
		|| echo "$$p: FAILED (run: databricks auth login --profile $$p)"; done

setup:
	$(RUN)/00_setup.py

smoke:
	$(RUN)/02_pyiceberg_smoke.py

probe:
	$(RUN)/01_probe.py --endpoint $(ENDPOINT) --workspaces $(WORKSPACES) $(PROBE_ARGS)

probe-all:
	@for e in $(SWEEP); do $(RUN)/01_probe.py --endpoint $$e --workspaces a $(PROBE_ARGS) || exit 1; \
		echo "cooldown 60s"; sleep 60; done

probe-scope:
	@for e in $(SCOPE_SWEEP); do $(RUN)/01_probe.py --endpoint $$e --workspaces a,b $(PROBE_ARGS) || exit 1; \
		echo "cooldown 60s"; sleep 60; done

# In-workspace runs (serverless job): removes the laptop's network from the measurement.
job-deploy:
	@set -- $(PROFILES); t=a; for p in "$$@"; do \
		(cd databricks && databricks bundle deploy --profile $$p --target $$t) || exit 1; t=b; done

probe-job:
	$(RUN)/06_run_in_workspace.py --endpoint $(ENDPOINT) --workspaces $(WORKSPACES) \
		$(if $(SP_SCOPE),--sp-scope $(SP_SCOPE)) -- $(PROBE_ARGS)

job-destroy:
	@set -- $(PROFILES); t=a; for p in "$$@"; do \
		(cd databricks && databricks bundle destroy --auto-approve --profile $$p --target $$t) || exit 1; t=b; done

analyze:
	$(RUN)/03_analyze.py

plot:
	$(RUN)/07_plot_scope.py

upload:
	$(RUN)/04_upload.py

dashboard:
	uv run python dashboard/build_dashboard.py
	$(RUN)/05_dashboard.py

cleanup: job-destroy
	$(RUN)/99_cleanup.py
