# target-postgres

A [Singer](https://www.singer.io/) target that loads data into PostgreSQL.
See `SPEC.md` for the full behavioral specification this implementation follows.

## Usage

```sh
uv sync
uv run target-postgres --config config.json < input.jsonl
```

Show plugin metadata (name, version, and supported capabilities) without reading stdin:

```sh
uv run target-postgres --about
```

## BATCH (Arrow) support

In addition to `RECORD` messages, this target accepts Singer `BATCH` messages whose
`encoding.format` is `"arrow"` -- e.g. from `tap-mysql` or `mapper-fivetran`. No
configuration is required: any `BATCH` message pointing at one or more Arrow
[IPC file format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format)
files is loaded automatically, using
[`adbc-driver-postgresql`](https://arrow.apache.org/adbc/) to bulk-ingest the Arrow
data directly into Postgres (no per-row Python materialization). `pyarrow` and
`adbc-driver-postgresql` are regular dependencies of this package - unlike some other
ADBC drivers (e.g. MySQL's), `adbc-driver-postgresql` is a normal pip-installable
wheel, so there's no separate native driver install step.

Manifest files are consumed once: each listed file is read, loaded, and then deleted
from disk (only after the full batch has been successfully loaded, so a failure
partway through leaves the source files intact for a retry). Table DDL/typing still
comes entirely from the stream's `SCHEMA` message, exactly as for `RECORD` messages --
Arrow data is matched into columns by name, with no separate Arrow-to-Postgres type
inference. Keyed streams (`key_properties` set) MERGE-upsert the same way `RECORD`
batches do; keyless streams append.

A few things worth knowing:

- **Flattening/inflection restriction**: `data_flattening_max_level > 0` and
  `underscore_camel_case_fields` are not supported for BATCH-sourced streams whose
  schema has nested object properties (or whose property names would be renamed by
  inflection) -- both rename columns away from the raw property names the Arrow file
  itself uses, and BATCH loading matches columns by name. Loading such a stream via
  BATCH raises a clear error; set `data_flattening_max_level` to `0` (the default) and
  `underscore_camel_case_fields` to `false` for that stream, or keep it on `RECORD`
  mode.
- **`_sdc_*` metadata columns** (`add_metadata_columns`/`hard_delete`) are handled
  per-column for BATCH-sourced records: `_sdc_batched_at` *is* populated (a wall-clock
  constant applied to every row in the batch, refreshed on each subsequent BATCH
  update of the same rows); `_sdc_extracted_at` is never populated, since BATCH has no
  per-record `time_extracted` equivalent to source a value from; `_sdc_deleted_at` is
  only populated if a tap's own Arrow schema happens to include that column (nothing
  target-side prevents it, but the tap-mysql/mapper-fivetran convention this was built
  against doesn't emit it). Whatever isn't populated is left untouched by BATCH-sourced
  updates rather than overwritten with `NULL`, so values from earlier `RECORD` syncs
  survive.
- **String value representation differs from `RECORD` mode**: `RECORD`-sourced string
  values are stored JSON-quoted (a quirk inherited from the original
  pipelinewise-target-postgres's CSV/`COPY` loading path -- see `SPEC.md` §15).
  BATCH/Arrow-sourced string values are stored as plain, unquoted text. This is new
  functionality with no prior behavior to preserve, and native values are the more
  correct choice going forward -- just be aware a column populated by both `RECORD`
  and `BATCH` messages over time will have this inconsistency.

## Tests

```sh
uv run pytest tests/unit           # no DB required
uv run pytest tests/integration    # requires Docker (testcontainers)
```
