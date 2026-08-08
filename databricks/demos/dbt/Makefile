# Deploy wrappers. Two rules encoded here:
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

.PHONY: deploy-dev deploy-prod deploy-prod-local

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
