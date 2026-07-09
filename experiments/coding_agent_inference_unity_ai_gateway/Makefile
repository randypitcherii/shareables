.DEFAULT_GOAL := help

.PHONY: help verify

help: ## List available targets
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

verify: ## Prove SSO auth end-to-end with one real chat completion (HTTP 200 required)
	uv run python scripts/verify.py
