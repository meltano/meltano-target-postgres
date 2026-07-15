# target-postgres — Clean-Room Re-implementation Spec

This document specifies the observable behavior of `pipelinewise-target-postgres`
(a [Singer](https://www.singer.io/) target) in enough detail to re-implement it
from scratch, without copying its code.

## 1. Overview

A Singer target reads a stream of newline-delimited JSON messages
(`SCHEMA`, `RECORD`, `STATE`, `ACTIVATE_VERSION`) from stdin and loads them into
PostgreSQL. Tables are created/altered automatically from the JSON Schema of
each stream, and rows are upserted based on the stream's declared key
properties.

Entry point: `target-postgres --config config.json`, reading Singer messages
from stdin, config from the JSON file (or `{}` if `-c/--config` is omitted).

## 2. Dependencies (current)

| Package | Version pinned | Purpose |
|---|---|---|
| `jsonschema` | `==3.2.0` | Draft7Validator + FormatChecker for optional record validation |
| `singer-python` | git fork (`Matatika/singer-python`) | logging (`get_logger`), metrics `Counter` |
| `psycopg2-binary` | `==2.9.5` | PostgreSQL driver, `COPY FROM STDIN`, `DictCursor` |
| `inflection` | `==0.3.1` | `underscore()` for camelCase → snake_case column inflection |
| `joblib` | `==1.2.0` | `Parallel`/`parallel_backend('threading')` to flush streams concurrently |

Test-only: `pytest==6.2.5`, `pylint==2.6.0`, `pytest-cov==2.10.1`.

### Packaging/dependency management

The reimplementation should use [`uv`](https://docs.astral.sh/uv/) instead of
`setup.py`/`pip`/`venv` for all dependency management:

- `pyproject.toml` (PEP 621) as the single source of truth for metadata,
  dependencies, and the `target-postgres` console-script entry point,
  replacing `setup.py`.
- `uv add <pkg>` / `uv add --dev pytest testcontainers[postgres]` to manage
  runtime vs. dev/test dependency groups, with `uv.lock` committed for
  reproducible installs (replacing the current unpinned/loosely-pinned `make
  venv` flow).
- `uv run pytest`, `uv run target-postgres --config config.json`, `uv run
  pylint ...` in place of `make venv && source .venv/bin/activate && ...` —
  no need for the venv to be manually activated in CI or locally.
- `uv sync` in CI to install exactly the locked dependency set instead of
  `pip install -e .[test]` against loose pins.
- Since `uv` resolves and locks fast, drop the exact `==` pins in favor of
  compatible-release ranges (e.g. `>=2.9,<3`) in `pyproject.toml` and let
  `uv.lock` pin the resolved versions — avoids the current situation where
  `jsonschema==3.2.0`/`inflection==0.3.1` are years behind with no easy way
  to bump them.

### Suggested modern replacements

| Old | Replace with | Why |
|---|---|---|
| `setup.py` + `pip`/`venv`/`make venv` | `uv` + `pyproject.toml` (see above) | single lockfile, fast resolution/installs, no manual venv activation |
| `psycopg2-binary` | `psycopg[binary]` (psycopg3) | actively maintained, native `COPY` async/sync API, better type adaptation; or keep `psycopg2-binary` unpinned if minimizing churn |
| `jsonschema==3.2.0` | `jsonschema>=4` (still `Draft7Validator`/`Draft202012Validator`) | 3.2.0 is years out of date; format checking API unchanged |
| `singer-python` fork | `singer-sdk` (Meltano) target base classes, or plain stdlib logging + a small metrics shim | fork is unofficial and unmaintained; singer-sdk gives you SCHEMA/RECORD/STATE parsing, config validation (via a JSON-schema-defined settings class), and CLI plumbing for free |
| `inflection==0.3.1` | `inflection>=0.5` (or hand-rolled regex, it's ~2 functions) | pinned version is ancient; only `underscore()` is used |
| `joblib` threading backend | `concurrent.futures.ThreadPoolExecutor` | stdlib, no dependency; joblib's parallel_backend indirection buys nothing here since it's always `threading` |
| Manual CSV + `COPY` | Keep `COPY ... WITH (FORMAT CSV)` (fastest bulk-load path) but consider `io.StringIO`/pipe instead of temp files, or psycopg3's `copy()` context manager which streams without a temp file at all |
| Hand-written `MERGE`-via-temp-table upsert | PostgreSQL `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` (native upsert, available since PG 9.5) | eliminates the temp table + separate UPDATE + INSERT statements; single round trip, no race window between UPDATE and INSERT |
| `argparse` for CLI | keep, or `click`/`typer` if adding more subcommands | not a real pain point today |

If adopting `singer-sdk`, note it changes wire-level details (e.g. config
validation errors, default batch sizing) — treat it as an architecture choice,
not a drop-in swap; validate against the integration tests below before
switching.

## 3. Configuration settings (`config.json`)

| Key | Type | Required | Default | Notes |
|---|---|---|---|---|
| `host` | string | **yes** | — | |
| `port` | integer | **yes** | — | |
| `user` | string | **yes** | — | |
| `password` | string | **yes** | — | |
| `dbname` | string | **yes** | — | |
| `default_target_schema` | string | conditionally | — | required unless `schema_mapping` covers every stream |
| `schema_mapping` | object | conditionally | — | keyed by the `<schema>` segment of `stream-id` (see §6); each entry: `{"target_schema": str, "target_schema_select_permissions": str\|list[str], "indices": {table_name: [col, ...]}}` |
| `default_target_schema_select_permissions` | string \| list[str] | no | — | grantees for `GRANT USAGE`/`GRANT SELECT` on newly created schema/tables |
| `batch_size_rows` | integer | no | `100000` | rows buffered (deduped by PK) before a stream is flushed |
| `flush_all_streams` | boolean | no | `false` | when a stream hits `batch_size_rows`, flush every buffered stream, not just that one |
| `parallelism` | integer | no | `0` | `0` = auto (one thread per stream being flushed, capped by `max_parallelism`); positive N = N threads |
| `max_parallelism` | integer | no | `16` | ceiling for auto parallelism |
| `add_metadata_columns` | boolean | no | `false` | adds `_sdc_extracted_at`, `_sdc_batched_at`, `_sdc_deleted_at` columns and populates them per record |
| `hard_delete` | boolean | no | `false` | implies metadata columns are populated (checked, not force-enabled in config — see §7); after each flush, `DELETE FROM tbl WHERE _sdc_deleted_at IS NOT NULL` |
| `data_flattening_max_level` | integer | no | `0` | depth of nested-object flattening into `parent__child` columns before falling back to `jsonb` |
| `primary_key_required` | boolean | no | `true` | if true, a `SCHEMA` message with empty `key_properties` raises |
| `validate_records` | boolean | no | `false` | validate each `RECORD` against its stream's Draft7 JSON Schema before buffering |
| `temp_dir` | string | no | platform default (`tempfile.mkstemp` default) | directory for per-batch CSV files |
| `underscore_camel_case_fields` | boolean | no | `false` | inflect camelCase/PascalCase property names to `snake_case` (with a special rule collapsing runs of capitals, e.g. `HTTPHeader_Value` → `http_header__value`) |
| `ssl` | string `"true"`/other | no | off | appends `sslmode=require` to the libpq connection string |

Config validation (`validate_config`) only checks: the 5 connection keys are
non-empty, and at least one of `default_target_schema` / `schema_mapping` is
set. It does **not** validate types, ranges, or connectivity — invalid
combinations fail later against Postgres itself.

## 4. Data type handling — JSON Schema → PostgreSQL column type

Mapping function operates on a single flattened property's schema
(`{"type": [...], "format": ..., "maximum": ...}`), in this precedence order:

1. `type` contains `object` or `array` → **`jsonb`**
2. `format == "date-time"` → **`timestamp without time zone`** (timezone info in the value is discarded/not detected)
3. `format == "time"` → **`time without time zone`**
4. `type` contains `number` → **`double precision`**
5. `type` contains both `integer` and `string` → **`character varying`** (ambiguous type unions fall back to string)
6. `type` contains `integer` (and not `string`):
   - if `maximum` present and `<= 32767` → **`smallint`**
   - elif `<= 2147483647` → **`integer`**
   - elif `<= 9223372036854775807` → **`bigint`**
   - if `maximum` absent → **`numeric`** (unbounded)
   - if `maximum` present but exceeds all three bounds above → **falls through with no type assigned in current code** (bug: leaves whatever `col_type` was already set to, i.e. `character varying` — worth fixing to `numeric` in the reimplementation)
7. `type` contains `boolean` → **`boolean`**
8. default / anything else (plain `string`, no format) → **`character varying`** (no length limit — always unbounded `varchar`, never `varchar(n)`)

Property `type` may be a plain string or a list (e.g. `["string", "null"]`);
`null` is ignored for type-mapping purposes — nullability is not enforced at
the column level (no `NOT NULL` is ever emitted).

Column and table identifiers are always lower-cased and double-quoted:
`"column_name"`. Postgres identifier length limit of 63 bytes
(`MAX_IDENTIFIER_LENGTH = 63`) is respected by the key-flattening/truncation
logic (§5).

## 5. Schema flattening & column naming

- Nested object properties are recursively flattened into `parent__child`
  column names (double underscore separator) up to `data_flattening_max_level`
  levels deep; beyond that level, or for `array` types, the value is stored
  as `jsonb` (JSON-encoded) rather than expanded into columns.
- Optional camelCase inflection (`underscore_camel_case_fields`): applies two
  regex passes before `inflection.underscore()`:
  - `([A-Z]+)_([A-Z][a-z])` → insert an extra underscore between an
    all-caps run and a following Capitalized word segment
  - `([a-z0-9])_([A-Z])` → insert an extra underscore between a
    lower/digit and a following capital
- **Identifier truncation**: if a flattened `sep.join(key_parts)` name
  reaches/exceeds 63 chars, the implementation abbreviates earlier path
  segments (camelize then strip lowercase letters, i.e. keep only the
  uppercase "initials"; falls back to the first 3 chars if that yields ≤1
  char) working left-to-right until it fits.
- **Duplicate-name resolution**: after flattening + truncation, if two
  properties in the same schema produce the same column name, later
  occurrences get a `__1`, `__2`, ... suffix appended (re-truncating to stay
  ≤63 chars). Resolution order is by *sorted* key name, so it is
  deterministic regardless of input property order.
- Record flattening mirrors schema flattening (same key-naming/truncation/
  dedup rules) but recurses into actual record values rather than schema
  nodes; any value flattened to a column whose schema type is exactly
  `{null, object, array}` (or any raw `dict`/`list` value) is JSON-serialized
  before being written to the CSV/inserted.
- Primary key columns are identified by the stream's `key_properties`,
  inflected/safe-named the same way as any other column.

## 6. Stream → schema/table name resolution

A stream name may encode schema/table as `<table>`, `<schema>-<table>`, or
`<catalog>-<schema>-<table...>` (splitting on `-`; extra segments after the
2nd are rejoined with `_` into the table name). This "schema" segment (not the
Postgres target schema) is the lookup key into `schema_mapping`.

Target schema resolution order per stream:
1. If `schema_mapping[stream_schema_name]` exists → its `target_schema`.
2. Else → top-level `default_target_schema`.
3. Else → error at DbSync construction time.

Grantees resolution: `schema_mapping[...].target_schema_select_permissions`
if present, else `default_target_schema_select_permissions`.

Table name on the wire: `.`/`-` in the stream's table segment are replaced
with `_`, then lower-cased, then quoted: `"table_name"`. Full qualified name
is `<schema>."<table>"`. Temp tables use a random name: `tmp_<uuid4 with
dashes replaced by underscores>` (unqualified, relies on session temp schema).

## 7. Message handling / ingestion loop

- `SCHEMA`: requires `stream` and `key_properties`. If
  `primary_key_required` (default true) and `key_properties` is empty →
  raise and abort. If a previous batch of records for this stream is
  pending, flush it *before* switching to the new schema. Then (re)creates a
  `DbSync` for the stream, ensures the target schema exists
  (`CREATE SCHEMA IF NOT EXISTS`, plus `GRANT USAGE` to configured grantees if
  the schema didn't already exist), and syncs the table: `CREATE TABLE IF NOT
  EXISTS` when absent (plus `GRANT SELECT` to grantees), or diff+`ALTER TABLE
  ADD COLUMN` for any new columns and, for any column whose Postgres type no
  longer matches the (re-)computed type, **rename the old column** to
  `"<name>_<YYYYMMDD_HHMM>"` (versioning/preserving old data) and add a new
  column with the current name/type — it does not attempt to cast/migrate
  data into the new type.
  - If `add_metadata_columns` or `hard_delete` is set, the 3 `_sdc_*`
    metadata properties (§8) are injected into the schema before it's used.
- `RECORD`: requires a prior `SCHEMA` for the stream (else raise). Optionally
  validated against the stream's Draft7Validator + FormatChecker (only if
  `validate_records`); floats in the record are first walked and converted to
  `Decimal` (via `Decimal(str(x))`) for accurate validation of e.g.
  `multipleOf` — a `decimal.InvalidOperation` during validation of a
  high-precision `multipleOf` is caught and re-raised as
  `InvalidValidationOperationException` with a clear message; any other
  validation failure raises `RecordValidationException`.
  - Records are buffered **keyed by their computed primary-key string**
    (`','.join(str(v) for v in key values)`, or a synthetic
    `RID-<total_row_count>` if the stream has no key properties) — so
    multiple RECORD messages for the same PK arriving before a flush
    **collapse into the last one seen** (last-write-wins de-dup within a
    batch, not counted twice).
  - When the buffered count for the stream reaches `batch_size_rows`, flush:
    either just this stream, or (if `flush_all_streams`) every buffered
    stream — using whichever the parallelism settings dictate (§9) — then
    emit the resulting STATE line to stdout immediately.
- `STATE`: stored as the latest known state; on the very first STATE seen,
  it seeds `flushed_state` too (so an early STATE isn't lost if no flush has
  happened yet).
- `ACTIVATE_VERSION`: **no per-table action is taken** — no version column,
  no table swap/truncate. The only effect is: if no state has been flushed
  yet, `flushed_state` is seeded from the last known `state`. This target
  does not implement "hard sync"/table versioning semantics some other
  targets provide for `ACTIVATE_VERSION` — it's effectively a no-op besides
  state bookkeeping. **Callers relying on ACTIVATE_VERSION to truncate/replace
  a table should not assume that here**; document this explicitly if you
  reimplement, since it's a common point of confusion vs. other Singer
  targets (e.g. target-snowflake perform table swaps on this message).
- Any other/missing `type` key → raise.
- End of input: if any stream has unflushed rows, flush all streams once
  more, then emit final state.

## 8. Metadata (`_sdc_*`) columns

Added only when `add_metadata_columns` or `hard_delete` is true, both to the
schema (as extra JSON Schema properties, so they get normal column-type
treatment) and to each record:

| Column | Type | Value |
|---|---|---|
| `_sdc_extracted_at` | `timestamp without time zone` | `time_extracted` field of the RECORD message (nullable string) |
| `_sdc_batched_at` | `timestamp without time zone` | `datetime.now().isoformat()` at flatten time (wall clock of the target process) |
| `_sdc_deleted_at` | `character varying` (no format ⇒ default type) | `record['_sdc_deleted_at']` if the tap sent one (used by log-based/CDC taps to flag deletes), else `None` |

## 9. Flushing, batching, and parallelism

- Buffering is per-stream, in-memory dict keyed by PK string; `batch_size_rows`
  (default 100000) is evaluated against the *deduped* count.
- On flush: `parallelism` (default 0 = auto) determines a thread count —
  auto mode uses `min(#streams_to_flush, max_parallelism)` (default cap 16);
  streams are flushed concurrently via a thread pool (`joblib`
  `parallel_backend('threading')`).
- Each stream's flush: write buffered records to a temp CSV file (one file
  per flush, name `<stream>_XXXXXX.csv` in `temp_dir` or the OS temp dir; each
  field JSON-encoded except omitted `NULL`/falsy-but-not-zero values, which
  become empty CSV fields), `COPY` it into a fresh Postgres temp table,
  perform the upsert (§10), create any configured indices on the real table,
  optionally hard-delete flagged rows, then delete the temp CSV file and
  reset the in-memory buffer for that stream to empty.
- `flush_all_streams=false` (default): only the stream that hit its batch
  threshold is flushed on an intermediate flush; `STATE` for the *other*
  streams isn't advanced (retained from `state['bookmarks'][stream]`) until
  they're eventually flushed too — this bounds memory use per-stream while
  keeping bookmarks conservative/correct per stream.
- `flush_all_streams=true`: every buffered stream is flushed whenever any one
  crosses its threshold, and the *entire* current `state` is used as the new
  `flushed_state` (not just the flushed streams' bookmarks).

## 10. Upserting

This is a merge (upsert), performed per flush, per stream, only when the
stream has ≥1 key property (`key_properties` non-empty):

1. `CREATE TEMP TABLE tmp_<uuid>` with the same flattened columns (no PK
   constraint on the temp table).
2. `COPY tmp_<uuid> (<flattened columns>) FROM STDIN WITH (FORMAT CSV, ESCAPE '\')`
   loading the batch's CSV file.
3. If the stream has key properties:
   `UPDATE <target> SET col=s.col, ... FROM tmp_<uuid> s WHERE <target>.pk1 =
   s.pk1 AND <target>.pk2 = s.pk2 ...` — updates **every** flattened column
   (not just non-PK ones) for rows whose PK already exists in the target.
   Row count of this statement is recorded as `updates` and reported to
   singer-python's `Counter('record_count', {'stream': stream})` metric.
4. `INSERT INTO <target> (<cols>) SELECT s.* FROM tmp_<uuid> s LEFT OUTER
   JOIN <target> t ON <pk match> WHERE <t.pk IS NULL for all pk cols>` —
   inserts only rows whose PK was *not* found in the target (anti-join),
   i.e. rows just updated in step 3 are excluded here. Row count reported as
   `inserts` to the same `record_count` counter.
5. If the stream has **no** key properties: skip step 3 entirely; step 4
   becomes a plain `INSERT INTO <target> (<cols>) SELECT s.* FROM tmp_<uuid>
   s` (append-only, no upsert, potential duplicates across syncs by design).
6. Temp table is dropped automatically at the end of the connection's
   session (Postgres `TEMP TABLE` semantics) — there is no explicit `DROP
   TABLE`.
7. Log line: `Loading into <schema>.<table>: {"inserts": N, "updates": M,
   "size_bytes": B}`.

Note this is **not** `INSERT ... ON CONFLICT DO UPDATE`; it's the
classic MERGE-via-temp-table pattern using 2 statements + a temp table +
COPY, presumably to support the no-PK append path and per-row bulk COPY
performance uniformly. A reimplementation could keep the temp-table+COPY
staging (COPY is still the fastest bulk load path) but replace steps 3+4
with a single `INSERT ... ON CONFLICT (pk...) DO UPDATE SET col=EXCLUDED.col,
...` when key properties exist, which removes the UPDATE/INSERT race window
and halves round trips.

## 11. Schema/table lifecycle details

- `create_schema_if_not_exists`: schema existence is checked case-insensitively
  against `information_schema.schemata`; if missing, `CREATE SCHEMA IF NOT
  EXISTS` then grants are applied.
- `sync_table`: table existence checked via `information_schema.tables`
  filtered by schema; if missing, `CREATE TABLE IF NOT EXISTS <cols>,
  PRIMARY KEY (<pk cols>)` (PK clause omitted entirely if no key properties)
  then SELECT grants applied; if present, `update_columns()` diffs against
  `information_schema.columns` (case-insensitive column name match) and:
  - adds any column present in the flattened schema but absent in Postgres
  - for any column present in both whose Postgres `data_type` (lower-cased)
    differs from the freshly computed type (lower-cased), **renames** the
    existing column (append `_<YYYYMMDD_HHMM>` timestamp) then adds a new
    column with the original name and the new type. Old data is preserved
    under the renamed column, not migrated/cast.
- Table/schema existence checks always hit Postgres live per stream sync (no
  persistent process-wide cache in the current code, despite a
  `disable_table_cache`-shaped test-config field and a
  `table_columns_cache` parameter plumbed into `create_schema_if_not_exists`
  that nothing currently populates — vestigial/dead code paths worth
  either removing or actually wiring up in a reimplementation).
- Indices: `default`/per-stream `indices` from `schema_mapping[...]indices
  [table]` (plus a fixed `_sdc_deleted_at` index whenever `hard_delete` is
  on) are created (`CREATE INDEX IF NOT EXISTS i_<table[:30]>_<col> ON
  <table> (<col>)`) once per flush, **after** the insert/update, not at
  table-creation time.

## 12. Connection handling

- One new `psycopg2` connection is opened per query/flush operation (`with
  self.open_connection() as connection:` — no pooling, no connection reuse
  across calls). Connection string is hand-built
  (`host='..' dbname='..' user='..' password='..' port='..'`), optionally
  appending `sslmode='require'` when `config['ssl'] == 'true'` (string
  comparison, not boolean).
- All queries use `DictCursor`. Rows are returned as a list only if
  `cur.rowcount > 0`, else `[]` (note: for statements like `UPDATE`/`INSERT`
  this repurposes `rowcount` for both "did it return rows" and "how many
  rows were affected", which happens to work for this codebase's usage but
  is worth making explicit/separate in a reimplementation).

## 13. What the test suites validate

### Unit tests (`tests/unit/`, no DB required)

- `test_db_sync.py`:
  - `validate_config`: required-keys + schema/schema_mapping presence checks.
  - `column_type`: full JSON-Schema-type → Postgres-type truth table (§4).
  - `stream_name_to_dict`: `-`-delimited stream name parsing into
    catalog/schema/table (0, 1, 2, 3+ segment cases).
  - `flatten_schema`: nesting up to `max_level`, falling back to jsonb past
    that level, `should_inflect` camelCase handling, key truncation at 63
    chars, dedup suffixing behavior.
  - `flatten_record`: same but for actual record dict values, incl.
    JSON-dumping of dict/list values and values typed as
    `{null,object,array}` in the accompanying flatten_schema.
- `test_target_postgres.py` (unit-level, DB calls mocked out): batching
  math — e.g. 40 records with `batch_size_rows=20` triggers exactly one
  intermediate flush (2nd batch flushed at end-of-stream), verifying
  `persist_lines` calls `flush_streams`/emits state the expected number of
  times without ever touching a live DB.

### Integration tests (`tests/integration/`, require a live Postgres — env vars `TARGET_POSTGRES_HOST/PORT/USER/PASSWORD/DBNAME/SCHEMA`)

- Malformed input: invalid JSON line raises `JSONDecodeError`; a RECORD
  before its SCHEMA raises.
- End-to-end load of multiple streams/tables with mixed column types from
  fixture tap output (`tests/integration/resources/*.json`), asserting the
  actual row contents land correctly typed in Postgres.
- `add_metadata_columns` on/off: metadata columns present/absent and
  correctly populated.
- `hard_delete`: soft-deleted rows (via `_sdc_deleted_at`) actually get
  `DELETE`d after load.
- Explicit `parallelism` values still produce correct end results.
- Multiple target schemas via `schema_mapping`.
- Reserved-word table/column names, table/column names containing spaces,
  non-DB-friendly (unicode/special char) column names — all safely quoted.
- Unicode character content round-trips correctly through the CSV/COPY path.
- Very long text values load without truncation (`character varying` has no
  length cap).
- Nested schema flattening vs. non-flattening (`data_flattening_max_level`)
  — verifying jsonb fallback vs. expanded columns.
- Column type change over time (`test_column_name_change`): a later SCHEMA
  with an incompatible type for an existing column results in the
  rename-old-column + add-new-column versioning behavior, and both old and
  new data are inspectable.
- `test_grant_privileges`: schema/table grants actually applied to a
  configured role.
- CDC/log-based replication behavior against a real logical-replication
  Postgres source, across default and small (`5`) `batch_size_rows`,
  including the zero-new-rows edge case.
- `flush_streams` unit-ish tests (mocking `emit_state`) verifying:
  intermediate flush emits state only for flushed streams (bookmarks for
  others held back) vs. `flush_all_streams=True` emitting/advancing every
  stream's bookmark together.
- `validate_records=True` causes bad records to raise
  `RecordValidationException`/`InvalidValidationOperationException` instead
  of silently reaching Postgres.
- `temp_dir` config actually places/uses the CSV files there.

### Test infra note (current)

Integration tests assume an already-running Postgres reachable via env vars
(`docker-compose up -d --build db` spins up `postgres:12-alpine` locally, and
CI is expected to provide equivalent connection details). There is no
per-test-process container lifecycle management in Python — the DB is an
external fixture.

## 14. Recommendation: integration tests via `testcontainers`

Replace the "external Postgres + env vars" fixture model with
[`testcontainers-python`](https://testcontainers-python.readthedocs.io/)
(`testcontainers[postgres]`), e.g.:

```python
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="session")
def pg_container():
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg

@pytest.fixture
def db_config(pg_container):
    return {
        "host": pg_container.get_container_host_ip(),
        "port": pg_container.get_exposed_port(5432),
        "user": pg_container.username,
        "password": pg_container.password,
        "dbname": pg_container.dbname,
        "default_target_schema": "public",
    }
```

Benefits worth calling out in the reimplementation's CONTRIBUTING docs:

- Tests are runnable with zero setup (`pytest`), no env vars, no
  `docker-compose up` step, no risk of polluting a shared/long-lived dev DB.
- CI matrix can trivially parameterize across Postgres major versions
  (12–17) by parametrizing the fixture's image tag — worth adding as a test
  matrix given this target's SQL is fairly vanilla but type-mapping/COPY
  edge cases can behave differently across major versions.
- Each test module/class can get an isolated container (function or
  class-scoped fixture) instead of relying on `DROP SCHEMA ... CASCADE`
  cleanup between tests against one shared long-lived instance — removes a
  whole class of test-order-dependence bugs the current suite is exposed to
  (its `setUp` drops the schema before every test, which is itself evidence
  the shared-DB model was already fighting isolation problems).
- Logical replication / CDC-flavored integration tests (currently pointing
  at a real external logical-replication-enabled Postgres) can spin up a
  purpose-configured container with `wal_level=logical` via
  `PostgresContainer(..., command="postgres -c wal_level=logical")` instead
  of depending on a pre-provisioned external instance.

## 15. Other gaps/considerations for a clean-room rewrite

- **No `NOT NULL` / `CHECK` constraints ever emitted** — every column is
  nullable and unconstrained beyond the primary key. Decide up front whether
  to preserve this (matches current behavior, keeps taps' evolving schemas
  from ever breaking loads) or tighten it — tightening is a behavior change
  that would break the "any schema evolves painlessly" guarantee the current
  target provides.
  - **Timestamp precision/timezone loss**: all `date-time` values are stored as
  `timestamp without time zone`, silently discarding timezone offsets in the
  source data. If input data spans multiple time zones this is lossy; the
  code has a standing `TODO` acknowledging it. Worth a deliberate decision
  (e.g. switch to `timestamptz`) rather than inheriting silently.
- **No SQL injection hardening on identifiers**: schema/table/column names
  are interpolated into f-strings/`.format()` after only quoting+lowercasing,
  not through a real identifier-escaping routine (e.g. doubling embedded
  `"` characters). A malicious/unusual stream or property name containing a
  `"` could break out of the quoted identifier. Use `psycopg2.sql.Identifier`
  (or psycopg3 `sql.Identifier`) throughout in the rewrite.
  - **Config values ARE inline-interpolated** into the `open_connection`
  connection string (host/dbname/user/password/port) rather than passed as
  a libpq keyword/value dict or DSN builder — again fine for well-formed
  config but worth using `psycopg2.extensions.make_dsn()`/a keyword-arg
  `connect()` call instead of manual string formatting.
- **`ssl` config is a string `"true"` check**, not a real boolean — easy to
  misconfigure (`"ssl": true` JSON boolean would silently not match).
  Reimplementation should accept a JSON boolean.
- **Metrics**: only a single singer-python `Counter('record_count', {'stream':
  ...})` is emitted around inserts+updates per flush; no counters for
  errors, schema changes, or timing. Consider whether the reimplementation
  wants structured timing metrics (COPY duration, upsert duration) for
  observability.
- **Vestigial/dead code**: `table_columns_cache` param on
  `create_schema_if_not_exists` and `disable_table_cache` config key
  referenced only in test scaffolding are unused/half-wired — don't carry
  these over unless actually implementing a real schema-cache.
- **Thread-safety of parallel flush**: each stream gets its own `DbSync`
  instance and opens its own connection per query, so concurrent flushes
  across *different* streams are safe, but there's no protection (nor is
  any needed today) against two flushes of the *same* stream overlapping —
  worth confirming that invariant still holds if you change the
  batching/flush scheduling.
- **No retry/backoff** around transient connection errors — a single failed
  query aborts the whole target process. Consider whether the
  reimplementation wants basic retry-with-backoff for connection blips,
  especially since bulk COPY + upsert per flush means a late failure loses
  the whole batch's progress (state for that batch was never emitted, so at
  least redelivery on tap restart is safe/idempotent given the upsert
  semantics — preserve that safety property).
- **CSV escaping is COPY's own (`ESCAPE '\'`, `FORMAT CSV`)**, and values are
  first JSON-encoded (`json.dumps(value, ensure_ascii=False)`) before being
  placed in a CSV field — meaning the DB actually receives JSON-encoded
  scalars for every column (e.g. a plain string `it's` becomes the JSON
  string `"it's"`, including quotes, then CSV-quoted again) and Postgres
  performs the input-type cast (`character varying` accepts a quoted JSON
  string as literal text including the quotes — need to check this
  produces the right value, i.e. whether the target actually strips the
  outer JSON quotes anywhere). **This is worth verifying carefully against
  the existing behavior with a byte-for-byte comparison test** before
  assuming a straightforward CSV writer (e.g. Python's `csv` module without
  the JSON-encoding step) is a safe swap — the JSON-encode step is almost
  certainly intentional (handles native `None`→omitted, nested
  dict/list→already-JSON-encoded jsonb payloads, and numeric/bool literals
  uniformly) but any rewrite must reproduce its exact quoting semantics for
  strings, especially values that already contain embedded quotes, commas,
  newlines, or backslashes.
