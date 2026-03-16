.PHONY: dev test verify lint clean

dev:
	uv sync --all-extras

test:
	uv run pytest tests/ -v

verify: lint test
	@echo "All checks passed."

lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

format:
	uv run ruff check --fix src/ tests/
	uv run ruff format src/ tests/

clean:
	rm -rf .venv __pycache__ dist build *.egg-info
