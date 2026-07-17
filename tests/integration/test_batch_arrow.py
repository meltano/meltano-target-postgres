import json
import os
from decimal import Decimal

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

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


def write_arrow_file(tmp_path, name, data: dict):
    table = pa.table(data)
    path = tmp_path / name
    with ipc.new_file(str(path), table.schema) as writer:
        writer.write_table(table)
    return str(path)


def batch_msg(stream, file_paths, encoding_format="arrow"):
    return json.dumps(
        {
            "type": "BATCH",
            "stream": stream,
            "encoding": {"format": encoding_format},
            "manifest": [f"file://{p}" for p in file_paths],
        }
    )


def fetch_rows(pg_connect, schema, table):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{schema}"."{table}" ORDER BY 1')
            columns = [d.name for d in cur.description]
            rows = cur.fetchall()
    return columns, rows


class TestBasicArrowLoad:
    def test_multi_column_load_lands_correctly_typed(self, db_config, pg_connect, tmp_path):
        arrow_path = write_arrow_file(
            tmp_path,
            "batch1.arrow",
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "amount": pa.array([9.5, 3.25], type=pa.float64()),
                "is_paid": pa.array([True, False]),
                "name": pa.array(["Ada", "Grace"]),
            },
        )
        lines = [
            schema_msg(
                "public-orders",
                {
                    "id": {"type": ["integer"]},
                    "amount": {"type": ["number"]},
                    "is_paid": {"type": ["boolean"]},
                    "name": {"type": ["string"]},
                },
            ),
            batch_msg("public-orders", [arrow_path]),
        ]

        persist_lines(db_config, lines)

        columns, rows = fetch_rows(pg_connect, "public", "orders")
        assert set(columns) == {"id", "amount", "is_paid", "name"}
        assert len(rows) == 2
        name_idx = columns.index("name")
        # Arrow-sourced strings land as plain unquoted text (unlike RECORD/CSV path's
        # JSON-quoted-text quirk, SPEC.md §15).
        names = {row[name_idx] for row in rows}
        assert names == {"Ada", "Grace"}

    def test_number_column_arrow_sourced_as_string_is_cast_to_target_type(self, db_config, pg_connect, tmp_path):
        # Mirrors tap-postgres's own Arrow BATCH encoding: a NUMERIC source column is
        # serialized as an Arrow string (utf8) to avoid float64 precision loss, even
        # though its SCHEMA message declares JSON Schema type "number" -- which this
        # target maps to a "double precision" column (schema.py's column_type). The
        # staging table adbc_ingest creates from the Arrow file is typed by Arrow's own
        # utf8 column, not the target's declared double precision, so the INSERT ...
        # SELECT from staging into the real table needs an explicit cast or Postgres
        # rejects it outright (text is not implicitly assignable to double precision).
        arrow_path = write_arrow_file(
            tmp_path,
            "batch1.arrow",
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "balance": pa.array(["9.50", "3.25"], type=pa.string()),
            },
        )
        lines = [
            schema_msg(
                "public-accounts",
                {
                    "id": {"type": ["integer"]},
                    "balance": {"type": ["number"]},
                },
            ),
            batch_msg("public-accounts", [arrow_path]),
        ]

        persist_lines(db_config, lines)

        columns, rows = fetch_rows(pg_connect, "public", "accounts")
        balance_idx = columns.index("balance")
        balances = {float(row[balance_idx]) for row in rows}
        assert balances == {9.5, 3.25}

    def test_arrow_decimal_column_is_ingestible(self, db_config, pg_connect, tmp_path):
        # Unlike tap-postgres (which serializes NUMERIC as an Arrow string, see the test
        # above), some taps encode a decimal source column as a native Arrow decimal type
        # (e.g. pipelinewise-tap-mysql for a MySQL DECIMAL column). adbc-driver-postgresql
        # can fail to even create the staging table for some decimal bit-widths
        # ("Can't map Arrow type 'decimal64' to Postgres type") before _upsert's cast ever
        # gets a chance to run -- _stringify_decimal_columns needs to convert it first.
        arrow_path = write_arrow_file(
            tmp_path,
            "batch1.arrow",
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "balance": pa.array([Decimal("9.50"), Decimal("3.25")], type=pa.decimal64(12, 2)),
            },
        )
        lines = [
            schema_msg(
                "public-decimal_accounts",
                {
                    "id": {"type": ["integer"]},
                    "balance": {"type": ["number"]},
                },
            ),
            batch_msg("public-decimal_accounts", [arrow_path]),
        ]

        persist_lines(db_config, lines)

        columns, rows = fetch_rows(pg_connect, "public", "decimal_accounts")
        balance_idx = columns.index("balance")
        balances = {float(row[balance_idx]) for row in rows}
        assert balances == {9.5, 3.25}

    def test_manifest_files_are_deleted_after_processing(self, db_config, tmp_path):
        arrow_path = write_arrow_file(tmp_path, "batch1.arrow", {"id": pa.array([1], type=pa.int64())})
        lines = [
            schema_msg("public-consumed", {"id": {"type": ["integer"]}}),
            batch_msg("public-consumed", [arrow_path]),
        ]

        persist_lines(db_config, lines)

        assert not os.path.exists(arrow_path)

    def test_keyless_stream_appends_via_batch(self, db_config, pg_connect, tmp_path):
        arrow_path = write_arrow_file(tmp_path, "batch1.arrow", {"id": pa.array([1, 1], type=pa.int64())})
        lines = [
            schema_msg("public-events", {"id": {"type": ["integer"]}}, key_properties=()),
            batch_msg("public-events", [arrow_path]),
        ]

        persist_lines({**db_config, "primary_key_required": False}, lines)

        _, rows = fetch_rows(pg_connect, "public", "events")
        assert len(rows) == 2


class TestArrowMergeUpsert:
    def test_overlapping_pks_across_batches_merge_upsert(self, db_config, pg_connect, tmp_path):
        first = write_arrow_file(
            tmp_path, "batch1.arrow", {"id": pa.array([1, 2], type=pa.int64()), "name": pa.array(["a", "b"])}
        )
        lines = [
            schema_msg("public-things", {"id": {"type": ["integer"]}, "name": {"type": ["string"]}}),
            batch_msg("public-things", [first]),
        ]
        persist_lines(db_config, lines)

        second = write_arrow_file(
            tmp_path, "batch2.arrow", {"id": pa.array([2, 3], type=pa.int64()), "name": pa.array(["b2", "c"])}
        )
        lines2 = [
            schema_msg("public-things", {"id": {"type": ["integer"]}, "name": {"type": ["string"]}}),
            batch_msg("public-things", [second]),
        ]
        persist_lines(db_config, lines2)

        columns, rows = fetch_rows(pg_connect, "public", "things")
        assert len(rows) == 3
        id_idx, name_idx = columns.index("id"), columns.index("name")
        by_id = {row[id_idx]: row[name_idx] for row in rows}
        assert by_id == {1: "a", 2: "b2", 3: "c"}

    def test_metadata_populated_by_record_survives_later_batch_update(self, db_config, pg_connect, tmp_path):
        config = {**db_config, "add_metadata_columns": True}
        lines = [
            schema_msg("public-widgets", {"id": {"type": ["integer"]}, "name": {"type": ["string"]}}),
            record_msg("public-widgets", {"id": 1, "name": "first"}, time_extracted="2024-01-01T00:00:00Z"),
        ]
        persist_lines(config, lines)

        columns, rows = fetch_rows(pg_connect, "public", "widgets")
        extracted_idx = columns.index("_sdc_extracted_at")
        id_idx = columns.index("id")
        before = {row[id_idx]: row[extracted_idx] for row in rows}
        assert before[1] is not None

        arrow_path = write_arrow_file(
            tmp_path, "batch1.arrow", {"id": pa.array([1], type=pa.int64()), "name": pa.array(["updated"])}
        )
        lines2 = [
            schema_msg("public-widgets", {"id": {"type": ["integer"]}, "name": {"type": ["string"]}}),
            batch_msg("public-widgets", [arrow_path]),
        ]
        persist_lines(config, lines2)

        columns, rows = fetch_rows(pg_connect, "public", "widgets")
        name_idx = columns.index("name")
        extracted_idx = columns.index("_sdc_extracted_at")
        id_idx = columns.index("id")
        after = {row[id_idx]: (row[name_idx], row[extracted_idx]) for row in rows}
        assert after[1][0] == "updated"
        # _sdc_extracted_at is never populated by BATCH, but must survive untouched
        # rather than being overwritten with NULL (the present-columns-only fix).
        assert after[1][1] == before[1]

    def test_sdc_batched_at_is_populated_for_batch_rows(self, db_config, pg_connect, tmp_path):
        arrow_path = write_arrow_file(
            tmp_path, "batch1.arrow", {"id": pa.array([1, 2], type=pa.int64())}
        )
        lines = [
            schema_msg("public-gadgets", {"id": {"type": ["integer"]}}),
            batch_msg("public-gadgets", [arrow_path]),
        ]

        persist_lines({**db_config, "add_metadata_columns": True}, lines)

        columns, rows = fetch_rows(pg_connect, "public", "gadgets")
        batched_idx = columns.index("_sdc_batched_at")
        assert all(row[batched_idx] is not None for row in rows)

    def test_sdc_batched_at_updates_on_later_batch(self, db_config, pg_connect, tmp_path):
        config = {**db_config, "add_metadata_columns": True}
        first = write_arrow_file(tmp_path, "batch1.arrow", {"id": pa.array([1], type=pa.int64())})
        lines = [
            schema_msg("public-gizmos", {"id": {"type": ["integer"]}}),
            batch_msg("public-gizmos", [first]),
        ]
        persist_lines(config, lines)

        columns, rows = fetch_rows(pg_connect, "public", "gizmos")
        batched_idx = columns.index("_sdc_batched_at")
        first_batched_at = rows[0][batched_idx]

        second = write_arrow_file(tmp_path, "batch2.arrow", {"id": pa.array([1], type=pa.int64())})
        lines2 = [
            schema_msg("public-gizmos", {"id": {"type": ["integer"]}}),
            batch_msg("public-gizmos", [second]),
        ]
        persist_lines(config, lines2)

        columns, rows = fetch_rows(pg_connect, "public", "gizmos")
        batched_idx = columns.index("_sdc_batched_at")
        assert rows[0][batched_idx] >= first_batched_at


class TestArrowErrorHandling:
    def test_unsupported_encoding_format_raises(self, db_config):
        from target_postgres.exceptions import UnsupportedBatchEncodingException

        lines = [
            schema_msg("public-things", {"id": {"type": ["integer"]}}),
            batch_msg("public-things", ["/tmp/does-not-matter.jsonl"], encoding_format="jsonl"),
        ]
        with pytest.raises(UnsupportedBatchEncodingException):
            persist_lines(db_config, lines)

    def test_flattening_incompatible_schema_raises(self, db_config, tmp_path):
        from target_postgres.exceptions import BatchFlatteningUnsupportedException

        arrow_path = write_arrow_file(tmp_path, "batch1.arrow", {"id": pa.array([1], type=pa.int64())})
        config = {**db_config, "data_flattening_max_level": 1}
        lines = [
            schema_msg(
                "public-nested",
                {
                    "id": {"type": ["integer"]},
                    "address": {
                        "type": ["object"],
                        "properties": {"city": {"type": ["string"]}},
                    },
                },
            ),
            batch_msg("public-nested", [arrow_path]),
        ]
        with pytest.raises(BatchFlatteningUnsupportedException):
            persist_lines(config, lines)
