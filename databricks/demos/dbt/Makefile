# Deploy wrappers exist mainly to stamp the human-readable `deployed_at` tag
# (THE_ONE_TRUE_WAY #12) -- DABs has no date function, so the shell provides it.
DEPLOY_DATE := $(shell date +%Y-%m-%d)

.PHONY: deploy-dev deploy-prod

deploy-dev:
	databricks bundle deploy -t dev --var="deployed_at=$(DEPLOY_DATE)"

deploy-prod:
	databricks bundle deploy -t prod --var="deployed_at=$(DEPLOY_DATE)"
