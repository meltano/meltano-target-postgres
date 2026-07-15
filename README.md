# target-postgres

A [Singer](https://www.singer.io/) target that loads data into PostgreSQL.
See `SPEC.md` for the full behavioral specification this implementation follows.

## Usage

```sh
uv sync
uv run target-postgres --config config.json < input.jsonl
```

## Tests

```sh
uv run pytest tests/unit           # no DB required
uv run pytest tests/integration    # requires Docker (testcontainers)
```
