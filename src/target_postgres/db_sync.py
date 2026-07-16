"""Postgres schema/table lifecycle and per-flush upsert (SPEC.md §9, §10, §11, §12).

Also handles Singer BATCH messages with encoding.format == "arrow": this is
net-new functionality with no equivalent section in SPEC.md, since BATCH support never
existed in the original pipelinewise-target-postgres.
"""

import json
import os
from datetime import datetime

import psycopg
from psycopg import sql

from target_postgres.arrow_batch import build_adbc_uri, read_manifest_tables
from target_postgres.csv_writer import records_to_csv
from target_postgres.exceptions import BatchFlatteningUnsupportedException
from target_postgres.logger import LOGGER
from target_postgres.naming import (
    resolve_grantees,
    resolve_indices,
    resolve_target_schema,
    safe_table_name,
    stream_name_to_dict,
    temp_table_name,
)
from target_postgres.schema import (
    add_metadata_columns_to_schema,
    column_type,
    flatten_schema,
)


class _ComposedSqlCursor:
    """Adapts a non-psycopg DBAPI cursor (e.g. ADBC's) so psycopg.sql.Composable
    objects built by _upsert/create_indices/_hard_delete render to plain SQL text
    before being handed to the underlying cursor, which doesn't understand
    psycopg.sql.Composable directly. Lets those methods run unmodified over either
    a real psycopg cursor or an ADBC one.
    """

    def __init__(self, cur):
        self._cur = cur

    def execute(self, query, params=None):
        if isinstance(query, sql.Composable):
            query = query.as_string(None)
        if params is None:
            return self._cur.execute(query)
        return self._cur.execute(query, params)

    def __getattr__(self, name):
        return getattr(self._cur, name)


class DbSync:
    """Handles schema/table sync and flush/upsert for a single stream."""

    def __init__(self, config: dict, stream_schema_message: dict):
        self.config = config
        self.stream = stream_schema_message["stream"]
        self.key_properties = stream_schema_message.get("key_properties") or []

        parsed_stream = stream_name_to_dict(self.stream)
        self.schema_name = resolve_target_schema(parsed_stream["schema_name"], config)
        self.grantees = resolve_grantees(parsed_stream["schema_name"], config)
        self.table = safe_table_name(parsed_stream["table_name"])
        self._stream_schema_segment = parsed_stream["schema_name"]

        properties = stream_schema_message["schema"].get("properties", {})
        self.raw_property_names = set(properties.keys())
        if config.get("add_metadata_columns") or config.get("hard_delete"):
            properties = add_metadata_columns_to_schema(properties)

        self.columns = flatten_schema(properties, config)
        self.column_types: dict[str, str] = {col["name"]: column_type(col["schema"]) for col in self.columns}
        self.pk_columns = self._pk_column_names()

    def _pk_column_names(self) -> list[str]:
        names = []
        for key_property in self.key_properties:
            for col in self.columns:
                if col["path"] == [key_property]:
                    names.append(col["name"])
                    break
        return names

    def _check_batch_flattening_supported(self):
        """BATCH-sourced Arrow files carry their tap-side (unflattened, un-inflected)
        column names -- e.g. a nested `c_obj` object stays a single `c_obj` column, and
        `HTTPHeader` stays `HTTPHeader` -- but if `data_flattening_max_level > 0` or
        `underscore_camel_case_fields` actually renamed a column away from its raw
        property name, by-name matching against the Arrow file can't bridge that gap.
        Fail clearly instead of silently dropping/misplacing that data.
        """
        flattened_names = {col["name"] for col in self.columns if not col["name"].startswith("_sdc_")}
        if flattened_names != self.raw_property_names:
            raise BatchFlatteningUnsupportedException(
                "data_flattening_max_level > 0 / underscore_camel_case_fields is not "
                f"supported for BATCH-sourced streams with nested or renamed properties "
                f"(stream {self.stream!r}). Set data_flattening_max_level=0 and "
                "underscore_camel_case_fields=false for this stream, or disable BATCH "
                "mode for it."
            )

    def column_order(self) -> list[str]:
        return [col["name"] for col in self.columns]

    def primary_key_string(self, record: dict, fallback_index: int) -> str:
        """Compute the buffering key for a RECORD (SPEC.md §7)."""
        if not self.key_properties:
            return f"RID-{fallback_index}"
        return ",".join(str(record.get(key_property)) for key_property in self.key_properties)

    # -- connection -----------------------------------------------------

    def _connect_kwargs(self) -> dict:
        kwargs = dict(
            host=self.config["host"],
            port=self.config["port"],
            user=self.config["user"],
            password=self.config["password"],
            dbname=self.config["dbname"],
        )
        if self.config.get("ssl") is True:
            kwargs["sslmode"] = "require"
        return kwargs

    def open_connection(self) -> psycopg.Connection:
        return psycopg.connect(**self._connect_kwargs())

    # -- schema/table lifecycle ------------------------------------------

    def create_schema_if_not_exists(self):
        with self.open_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.schemata WHERE lower(schema_name) = lower(%s)",
                    (self.schema_name,),
                )
                exists = cur.fetchone() is not None
                if not exists:
                    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema_name)))
                    self._grant_usage_on_schema(cur)
            conn.commit()

    def sync_table(self):
        with self.open_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND lower(table_name) = lower(%s)",
                    (self.schema_name, self.table),
                )
                exists = cur.fetchone() is not None
                if not exists:
                    self._create_table(cur)
                    self._grant_select_on_table(cur)
                else:
                    self._update_columns(cur)
            conn.commit()

    def _grantee_list(self) -> list:
        grantees = self.grantees
        if not grantees:
            return []
        if isinstance(grantees, str):
            return [grantees]
        return list(grantees)

    def _grant_usage_on_schema(self, cur):
        for grantee in self._grantee_list():
            cur.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(self.schema_name), sql.Identifier(grantee)
                )
            )

    def _grant_select_on_table(self, cur):
        for grantee in self._grantee_list():
            cur.execute(
                sql.SQL("GRANT SELECT ON {}.{} TO {}").format(
                    sql.Identifier(self.schema_name),
                    sql.Identifier(self.table),
                    sql.Identifier(grantee),
                )
            )

    def _create_table(self, cur):
        column_defs = sql.SQL(", ").join(
            sql.SQL("{} {}").format(
                sql.Identifier(name),
                sql.SQL(self.column_types[name]),  # ty:ignore[invalid-argument-type]
            )
            for name in self.column_order()
        )
        pk_clause = sql.SQL("")
        if self.pk_columns:
            pk_clause = sql.SQL(", PRIMARY KEY ({})").format(
                sql.SQL(", ").join(sql.Identifier(c) for c in self.pk_columns)
            )
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({}{})").format(
                sql.Identifier(self.schema_name),
                sql.Identifier(self.table),
                column_defs,
                pk_clause,
            )
        )

    def _update_columns(self, cur):
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND lower(table_name) = lower(%s)",
            (self.schema_name, self.table),
        )
        existing = {name.lower(): data_type.lower() for name, data_type in cur.fetchall()}

        for name in self.column_order():
            want_type = self.column_types[name]
            if name.lower() not in existing:
                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                        sql.Identifier(self.schema_name),
                        sql.Identifier(self.table),
                        sql.Identifier(name),
                        sql.SQL(want_type),  # ty:ignore[invalid-argument-type]
                    )
                )
            elif existing[name.lower()] != want_type.lower():
                timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M")
                renamed = f"{name}_{timestamp_suffix}"
                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} RENAME COLUMN {} TO {}").format(
                        sql.Identifier(self.schema_name),
                        sql.Identifier(self.table),
                        sql.Identifier(name),
                        sql.Identifier(renamed),
                    )
                )
                cur.execute(
                    sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                        sql.Identifier(self.schema_name),
                        sql.Identifier(self.table),
                        sql.Identifier(name),
                        sql.SQL(want_type),  # ty:ignore[invalid-argument-type]
                    )
                )

    def create_indices(self, cur):
        columns_to_index = list(resolve_indices(self._stream_schema_segment, self.table, self.config))
        if self.config.get("hard_delete") and "_sdc_deleted_at" not in columns_to_index:
            columns_to_index.append("_sdc_deleted_at")

        for col in columns_to_index:
            index_name = f"i_{self.table[:30]}_{col}"
            cur.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
                    sql.Identifier(index_name),
                    sql.Identifier(self.schema_name),
                    sql.Identifier(self.table),
                    sql.Identifier(col),
                )
            )

    # -- flush / upsert ---------------------------------------------------

    def flush(self, records: dict) -> dict:
        """records: pk-string -> flattened record dict. Returns {inserts, updates, size_bytes}."""
        if not records:
            return {"inserts": 0, "updates": 0, "size_bytes": 0}

        columns = self.column_order()
        csv_payload = records_to_csv(list(records.values()), columns)
        size_bytes = len(csv_payload.encode("utf-8"))
        tmp_table = temp_table_name()

        with self.open_connection() as conn:
            with conn.cursor() as cur:
                self._create_temp_table(cur, tmp_table, columns)
                self._copy_into(cur, tmp_table, columns, csv_payload)
                inserts, updates = self._upsert(cur, tmp_table, columns)
                self.create_indices(cur)
                if self.config.get("hard_delete"):
                    self._hard_delete(cur)
            conn.commit()

        result = {"inserts": inserts, "updates": updates, "size_bytes": size_bytes}
        LOGGER.info("Loading into %s.%s: %s", self.schema_name, self.table, json.dumps(result))
        return result

    def _create_temp_table(self, cur, tmp_table: str, columns: list[str]):
        column_defs = sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(self.column_types[c]))  # ty:ignore[invalid-argument-type]
            for c in columns
        )
        cur.execute(sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(sql.Identifier(tmp_table), column_defs))

    def _copy_into(self, cur, tmp_table: str, columns: list[str], csv_payload: str):
        col_list = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, ESCAPE '\\')").format(
            sql.Identifier(tmp_table),
            col_list,
        )
        with cur.copy(copy_stmt) as copy:
            copy.write(csv_payload)

    def _upsert(self, cur, tmp_table: str, columns: list[str], source_columns=None, computed_columns=None) -> tuple:
        """`source_columns` restricts the INSERT/SET column list to columns that actually
        exist in `tmp_table` - defaults to `columns` (all of them), matching flush()'s
        CSV/COPY temp table, which always has exactly `columns`. BATCH-sourced Arrow
        staging tables may be missing some target columns (e.g. _sdc_* metadata, never
        populated by BATCH); those are omitted from the column list entirely rather than
        explicitly set to NULL, so pre-existing values on updated rows are left untouched.

        `computed_columns` (name -> a psycopg.sql.Composable value expression) lets a
        caller fill in a column that's absent from `tmp_table` with a computed value
        instead of omitting it entirely -- e.g. `_sdc_batched_at` for BATCH-sourced rows,
        which has no per-row source data but is still a meaningful wall-clock constant.
        """
        if source_columns is None:
            source_columns = columns
        computed_columns = computed_columns or {}
        source_set = set(source_columns)
        cols = [c for c in columns if c in source_set or c in computed_columns]
        col_idents = [sql.Identifier(c) for c in cols]
        select_exprs = [sql.Identifier(c) if c in source_set else computed_columns[c] for c in cols]

        if not self.pk_columns:
            cur.execute(
                sql.SQL("INSERT INTO {schema}.{target_table} ({cols}) SELECT {select} FROM {temp_table}").format(
                    schema=sql.Identifier(self.schema_name),
                    target_table=sql.Identifier(self.table),
                    temp_table=sql.Identifier(tmp_table),
                    cols=sql.SQL(", ").join(col_idents),
                    select=sql.SQL(", ").join(select_exprs),
                )
            )
            return cur.rowcount, 0

        pk_idents = [sql.Identifier(c) for c in self.pk_columns]
        set_clause = sql.SQL(", ").join(sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in cols)
        stmt = sql.SQL(
            "WITH ins AS ("
            "INSERT INTO {schema}.{table} ({cols}) SELECT {select} FROM {tmp} "
            "ON CONFLICT ({pks}) DO UPDATE SET {set_clause} "
            "RETURNING (xmax = 0) AS inserted"
            ") SELECT "
            "count(*) FILTER (WHERE inserted) AS inserts, "
            "count(*) FILTER (WHERE NOT inserted) AS updates "
            "FROM ins"
        ).format(
            schema=sql.Identifier(self.schema_name),
            table=sql.Identifier(self.table),
            cols=sql.SQL(", ").join(col_idents),
            select=sql.SQL(", ").join(select_exprs),
            tmp=sql.Identifier(tmp_table),
            pks=sql.SQL(", ").join(pk_idents),
            set_clause=set_clause,
        )
        cur.execute(stmt)
        inserts, updates = cur.fetchone()
        return inserts or 0, updates or 0

    def _hard_delete(self, cur):
        cur.execute(
            sql.SQL("DELETE FROM {}.{} WHERE {} IS NOT NULL").format(
                sql.Identifier(self.schema_name),
                sql.Identifier(self.table),
                sql.Identifier("_sdc_deleted_at"),
            )
        )

    # -- BATCH (Arrow) ingestion -------------------------------------------

    def load_rows_from_arrow_files(self, file_paths: list) -> dict:
        """Load one or more Arrow IPC files from a Singer BATCH message's manifest
        (encoding.format == "arrow") directly into the target table via Postgres's
        ADBC driver -- no per-row Python materialization. Net-new functionality
        (MEL-535); no equivalent section in SPEC.md, since the original target never
        supported BATCH.

        Table DDL/typing still comes entirely from the stream's SCHEMA message
        (self.column_types); the Arrow file's own (tap-side) column names are matched
        by name against the target's columns via `_upsert`'s `source_columns`, which
        tolerates extra/missing columns on either side. Metadata columns absent from
        the Arrow file are handled per-column: `_sdc_batched_at` is filled in with a
        computed wall-clock constant (see `computed_columns` below); `_sdc_deleted_at`
        and `_sdc_extracted_at` are left untouched on updates and default to NULL on
        inserts (never explicitly overwritten with NULL) -- `_sdc_deleted_at` would
        already flow through automatically if a tap's Arrow schema happened to include
        it (nothing target-side prevents that), and `_sdc_extracted_at` has no BATCH
        equivalent of RECORD's per-record `time_extracted` to source a value from.

        Manifest files are only deleted after the full load succeeds, so a failure
        partway through a multi-file batch leaves every source file intact for a retry.

        Returns:
            {"inserts": N, "updates": M, "size_bytes": B}, matching flush()'s shape.
        """
        self._check_batch_flattening_supported()

        import adbc_driver_postgresql.dbapi as adbc_dbapi

        table = read_manifest_tables(file_paths)
        staging_table = temp_table_name()
        columns = self.column_order()

        conn = adbc_dbapi.connect(build_adbc_uri(self.config), autocommit=True)
        try:
            raw_cur = conn.cursor()
            raw_cur.adbc_ingest(
                staging_table,
                table,
                mode="create",
                db_schema_name=self.schema_name,
                temporary=True,
            )
            raw_cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
                [staging_table],
            )
            staging_columns = {row[0] for row in raw_cur.fetchall()}

            computed_columns = {}
            if "_sdc_batched_at" in columns and "_sdc_batched_at" not in staging_columns:
                # No per-row source data for this one (unlike _sdc_extracted_at, which
                # would need a time_extracted the BATCH protocol doesn't carry), but it's
                # a meaningful wall-clock constant we can fill in for every row in the
                # batch -- computed once here (target-process time, matching flush()'s
                # add_metadata_values_to_record) rather than via a DB-side now().
                computed_columns["_sdc_batched_at"] = sql.Literal(datetime.now().isoformat())

            cur = _ComposedSqlCursor(raw_cur)
            inserts, updates = self._upsert(
                cur,
                staging_table,
                columns,
                source_columns=staging_columns,
                computed_columns=computed_columns,
            )
            self.create_indices(cur)
            if self.config.get("hard_delete"):
                LOGGER.warning(
                    "hard_delete is enabled but _sdc_deleted_at is never populated for "
                    "BATCH-sourced records (stream %s); this pass only affects rows "
                    "already soft-deleted via prior RECORD messages.",
                    self.stream,
                )
                self._hard_delete(cur)
        finally:
            conn.close()

        for file_path in file_paths:
            try:
                os.remove(file_path)
            except OSError as exc:
                LOGGER.warning("Could not remove processed batch file %s: %s", file_path, exc)

        result = {"inserts": inserts, "updates": updates, "size_bytes": table.nbytes}
        LOGGER.info("Loading into %s.%s: %s", self.schema_name, self.table, json.dumps(result))
        return result
