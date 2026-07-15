venv:
	uv sync

lint:
	uv run ruff check src/

unit_test:
	uv run pytest tests/unit --cov=target_postgres --cov-report=html --cov-fail-under=60 $(extra_args)

integration_test:
	uv run pytest tests/integration --cov=target_postgres --cov-report=html $(extra_args) -vvv
