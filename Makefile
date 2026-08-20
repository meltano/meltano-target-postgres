.venv:
	uvx --with tox-uv tox devenv -e test .venv

lint:
	uvx --with tox-uv tox -e lint

unit_test:
	uvx --with tox-uv tox -e test -- tests/unit --cov=target_postgres --cov-report=html --cov-fail-under=60 $(extra_args)

integration_test:
	uvx --with tox-uv tox -e test -- tests/integration --cov=target_postgres --cov-report=html $(extra_args)
