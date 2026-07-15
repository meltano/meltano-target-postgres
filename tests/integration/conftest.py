import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer


@pytest.fixture(scope="session")
def pg_container():
    with PostgresContainer("postgres:18-alpine") as pg:
        yield pg


@pytest.fixture
def db_config(pg_container):
    return {
        "host": pg_container.get_container_host_ip(),
        "port": int(pg_container.get_exposed_port(5432)),
        "user": pg_container.username,
        "password": pg_container.password,
        "dbname": pg_container.dbname,
        "default_target_schema": "public",
    }


@pytest.fixture
def pg_connect(db_config):
    def _connect():
        return psycopg.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            dbname=db_config["dbname"],
        )

    return _connect


@pytest.fixture(autouse=True)
def clean_public_schema(pg_connect):
    yield
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE")
            cur.execute("CREATE SCHEMA public")
        conn.commit()
