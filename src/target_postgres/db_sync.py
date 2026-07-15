"""Postgres schema/table lifecycle and per-flush upsert (SPEC.md §9, §10, §11, §12)."""

import json
from datetime import datetime

import psycopg
from psycopg import sql

from target_postgres.csv_writer import records_to_csv
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
        if config.get("add_metadata_columns") or config.get("hard_delete"):
            properties = add_metadata_columns_to_schema(properties)

        self.columns = flatten_schema(properties, config)
        self.column_types = {
            col["name"]: column_type(col["schema"]) for col in self.columns
        }
        self.pk_columns = self._pk_column_names()

    def _pk_column_names(self) -> list[str]:
        names = []
        for key_property in self.key_properties:
            for col in self.columns:
                if col["path"] == [key_property]:
                    names.append(col["name"])
                    break
        return names

    def column_order(self) -> list[str]:
        return [col["name"] for col in self.columns]

    def primary_key_string(self, record: dict, fallback_index: int) -> str:
        """Compute the buffering key for a RECORD (SPEC.md §7)."""
        if not self.key_properties:
            return f"RID-{fallback_index}"
        return ",".join(
            str(record.get(key_property)) for key_property in self.key_properties
        )

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
                    cur.execute(
                        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                            sql.Identifier(self.schema_name)
                        )
                    )
                    self._grant_usage_on_schema(cur)
            conn.commit()

    def sync_table(self):
        with self.open_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND lower(table_name) = lower(%s)",
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
        existing = {
            name.lower(): data_type.lower() for name, data_type in cur.fetchall()
        }

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
        columns_to_index = list(
            resolve_indices(self._stream_schema_segment, self.table, self.config)
        )
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
        LOGGER.info(
            "Loading into %s.%s: %s", self.schema_name, self.table, json.dumps(result)
        )
        return result

    def _create_temp_table(self, cur, tmp_table: str, columns: list[str]):
        column_defs = sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(self.column_types[c]))  # ty:ignore[invalid-argument-type]
            for c in columns
        )
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(
                sql.Identifier(tmp_table), column_defs
            )
        )

    def _copy_into(self, cur, tmp_table: str, columns: list[str], csv_payload: str):
        col_list = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
        copy_stmt = sql.SQL(
            "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, ESCAPE '\\')"
        ).format(sql.Identifier(tmp_table), col_list)
        with cur.copy(copy_stmt) as copy:
            copy.write(csv_payload)

    def _upsert(self, cur, tmp_table: str, columns: list[str]) -> tuple:
        col_idents = [sql.Identifier(c) for c in columns]

        if not self.pk_columns:
            cur.execute(
                sql.SQL("INSERT INTO {}.{} ({cols}) SELECT {cols} FROM {}").format(
                    sql.Identifier(self.schema_name),
                    sql.Identifier(self.table),
                    sql.Identifier(tmp_table),
                    cols=sql.SQL(", ").join(col_idents),
                )
            )
            return cur.rowcount, 0

        pk_idents = [sql.Identifier(c) for c in self.pk_columns]
        set_clause = sql.SQL(", ").join(
            sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in columns
        )
        stmt = sql.SQL(
            "WITH ins AS ("
            "INSERT INTO {schema}.{table} ({cols}) SELECT {cols} FROM {tmp} "
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
