import json

from target_postgres.target import persist_lines


def schema_msg(stream, properties, key_properties=("id",)):
    return json.dumps(
        {
            "type": "SCHEMA",
            "stream": stream,
            "key_properties": list(key_properties),
            "schema": {"properties": properties},
        }
    )


def record_msg(stream, record, time_extracted=None):
    msg = {"type": "RECORD", "stream": stream, "record": record}
    if time_extracted:
        msg["time_extracted"] = time_extracted
    return json.dumps(msg)


def state_msg(value):
    return json.dumps({"type": "STATE", "value": value})


def fetch_rows(pg_connect, schema, table):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{schema}"."{table}" ORDER BY 1')
            columns = [d.name for d in cur.description]
            rows = cur.fetchall()
    return columns, rows


def fetch_column_types(pg_connect, schema, table):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (schema, table),
            )
            return dict(cur.fetchall())


class TestBasicLoad:
    def test_multi_stream_load_with_mixed_types(self, db_config, pg_connect):
        lines = [
            schema_msg(
                "public-orders",
                {
                    "id": {"type": ["integer"]},
                    "amount": {"type": ["number"]},
                    "is_paid": {"type": ["boolean"]},
                    "placed_at": {"type": ["string"], "format": "date-time"},
                },
            ),
            record_msg(
                "public-orders",
                {
                    "id": 1,
                    "amount": 9.99,
                    "is_paid": True,
                    "placed_at": "2024-01-01T00:00:00Z",
                },
            ),
            record_msg(
                "public-orders",
                {
                    "id": 2,
                    "amount": 19.5,
                    "is_paid": False,
                    "placed_at": "2024-01-02T00:00:00Z",
                },
            ),
            state_msg({"bookmarks": {"public-orders": {"pos": 2}}}),
        ]

        final_state = persist_lines(db_config, lines)

        assert final_state == {"bookmarks": {"public-orders": {"pos": 2}}}
        columns, rows = fetch_rows(pg_connect, "public", "orders")
        assert set(columns) == {"id", "amount", "is_paid", "placed_at"}
        assert len(rows) == 2

    def test_upsert_updates_existing_row(self, db_config, pg_connect):
        lines = [
            schema_msg(
                "public-things",
                {"id": {"type": ["integer"]}, "name": {"type": ["string"]}},
            ),
            record_msg("public-things", {"id": 1, "name": "first"}),
        ]
        persist_lines(db_config, lines)

        lines2 = [
            schema_msg(
                "public-things",
                {"id": {"type": ["integer"]}, "name": {"type": ["string"]}},
            ),
            record_msg("public-things", {"id": 1, "name": "updated"}),
            record_msg("public-things", {"id": 2, "name": "second"}),
        ]
        persist_lines(db_config, lines2)

        columns, rows = fetch_rows(pg_connect, "public", "things")
        assert len(rows) == 2
        id_idx, name_idx = columns.index("id"), columns.index("name")
        values_by_id = {row[id_idx]: row[name_idx] for row in rows}
        assert values_by_id[1] == '"updated"'

    def test_no_key_properties_is_append_only(self, db_config, pg_connect):
        lines = [
            schema_msg(
                "public-events", {"id": {"type": ["integer"]}}, key_properties=()
            ),
            record_msg("public-events", {"id": 1}),
            record_msg("public-events", {"id": 1}),
        ]
        persist_lines({**db_config, "primary_key_required": False}, lines)

        _, rows = fetch_rows(pg_connect, "public", "events")
        assert len(rows) == 2


class TestMetadataColumns:
    def test_metadata_columns_populated_when_enabled(self, db_config, pg_connect):
        lines = [
            schema_msg("public-widgets", {"id": {"type": ["integer"]}}),
            record_msg(
                "public-widgets", {"id": 1}, time_extracted="2024-01-01T00:00:00Z"
            ),
        ]
        persist_lines({**db_config, "add_metadata_columns": True}, lines)

        columns, _ = fetch_rows(pg_connect, "public", "widgets")
        assert "_sdc_extracted_at" in columns
        assert "_sdc_batched_at" in columns
        assert "_sdc_deleted_at" in columns

    def test_metadata_columns_absent_when_disabled(self, db_config, pg_connect):
        lines = [
            schema_msg("public-widgets", {"id": {"type": ["integer"]}}),
            record_msg("public-widgets", {"id": 1}),
        ]
        persist_lines(db_config, lines)

        columns, _ = fetch_rows(pg_connect, "public", "widgets")
        assert "_sdc_extracted_at" not in columns


class TestHardDelete:
    def test_soft_deleted_rows_are_hard_deleted(self, db_config, pg_connect):
        lines = [
            schema_msg("public-widgets", {"id": {"type": ["integer"]}}),
            record_msg("public-widgets", {"id": 1}),
            record_msg("public-widgets", {"id": 2}),
        ]
        persist_lines({**db_config, "hard_delete": True}, lines)

        lines2 = [
            schema_msg("public-widgets", {"id": {"type": ["integer"]}}),
            record_msg(
                "public-widgets", {"id": 1, "_sdc_deleted_at": "2024-01-01T00:00:00"}
            ),
        ]
        persist_lines({**db_config, "hard_delete": True}, lines2)

        columns, rows = fetch_rows(pg_connect, "public", "widgets")
        id_idx = columns.index("id")
        remaining_ids = {row[id_idx] for row in rows}
        assert remaining_ids == {2}


class TestSchemaMapping:
    def test_multiple_target_schemas(self, db_config, pg_connect):
        config = {
            **db_config,
            "schema_mapping": {
                "sales": {"target_schema": "sales_schema"},
                "hr": {"target_schema": "hr_schema"},
            },
        }
        lines = [
            schema_msg("sales-orders", {"id": {"type": ["integer"]}}),
            record_msg("sales-orders", {"id": 1}),
            schema_msg("hr-employees", {"id": {"type": ["integer"]}}),
            record_msg("hr-employees", {"id": 1}),
        ]
        persist_lines(config, lines)

        _, sales_rows = fetch_rows(pg_connect, "sales_schema", "orders")
        _, hr_rows = fetch_rows(pg_connect, "hr_schema", "employees")
        assert len(sales_rows) == 1
        assert len(hr_rows) == 1

        with pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA sales_schema CASCADE")
                cur.execute("DROP SCHEMA hr_schema CASCADE")
            conn.commit()


class TestColumnTypeChange:
    def test_incompatible_type_change_renames_old_column(self, db_config, pg_connect):
        lines = [
            schema_msg(
                "public-metrics",
                {"id": {"type": ["integer"]}, "value": {"type": ["integer"]}},
            ),
            record_msg("public-metrics", {"id": 1, "value": 42}),
        ]
        persist_lines(db_config, lines)
        assert fetch_column_types(pg_connect, "public", "metrics")["value"] == "numeric"

        lines2 = [
            schema_msg(
                "public-metrics",
                {"id": {"type": ["integer"]}, "value": {"type": ["string"]}},
            ),
            record_msg("public-metrics", {"id": 1, "value": "not a number"}),
        ]
        persist_lines(db_config, lines2)

        types = fetch_column_types(pg_connect, "public", "metrics")
        assert types["value"] == "character varying"
        renamed_columns = [c for c in types if c.startswith("value_") and c != "value"]
        assert len(renamed_columns) == 1


class TestIdentifierSafety:
    def test_reserved_word_and_unicode_names_round_trip(self, db_config, pg_connect):
        lines = [
            schema_msg(
                "public-select",
                {"order": {"type": ["integer"]}, "ünicöde": {"type": ["string"]}},
                key_properties=("order",),
            ),
            record_msg("public-select", {"order": 1, "ünicöde": "café"}),
        ]
        persist_lines(db_config, lines)

        columns, rows = fetch_rows(pg_connect, "public", "select")
        assert "order" in columns
        assert "café" in str(rows[0][columns.index("ünicöde")])
