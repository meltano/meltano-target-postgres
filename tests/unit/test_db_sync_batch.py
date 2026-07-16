import pytest

from target_postgres.db_sync import DbSync
from target_postgres.exceptions import BatchFlatteningUnsupportedException


def _stream_schema_message(properties, key_properties=("id",)):
    return {
        "stream": "public-things",
        "key_properties": list(key_properties),
        "schema": {"properties": properties},
    }


class TestCheckBatchFlatteningSupported:
    def test_flat_schema_is_supported(self):
        message = _stream_schema_message({"id": {"type": ["integer"]}, "name": {"type": ["string"]}})
        db_sync = DbSync({"default_target_schema": "public"}, message)
        db_sync._check_batch_flattening_supported()  # should not raise

    def test_metadata_columns_are_ignored(self):
        message = _stream_schema_message({"id": {"type": ["integer"]}})
        config = {"default_target_schema": "public", "add_metadata_columns": True}
        db_sync = DbSync(config, message)
        db_sync._check_batch_flattening_supported()  # should not raise

    def test_nested_flattening_raises(self):
        message = _stream_schema_message(
            {
                "id": {"type": ["integer"]},
                "address": {
                    "type": ["object"],
                    "properties": {"city": {"type": ["string"]}},
                },
            }
        )
        config = {"default_target_schema": "public", "data_flattening_max_level": 1}
        db_sync = DbSync(config, message)
        with pytest.raises(BatchFlatteningUnsupportedException):
            db_sync._check_batch_flattening_supported()

    def test_nested_object_without_flattening_is_supported(self):
        # data_flattening_max_level defaults to 0, so the nested object stays a single
        # jsonb column named exactly like the raw property -- no rename, BATCH-safe.
        message = _stream_schema_message(
            {
                "id": {"type": ["integer"]},
                "address": {
                    "type": ["object"],
                    "properties": {"city": {"type": ["string"]}},
                },
            }
        )
        db_sync = DbSync({"default_target_schema": "public"}, message)
        db_sync._check_batch_flattening_supported()  # should not raise

    def test_camel_case_inflection_raises(self):
        message = _stream_schema_message({"HTTPHeader": {"type": ["string"]}}, key_properties=())
        config = {"default_target_schema": "public", "underscore_camel_case_fields": True}
        db_sync = DbSync(config, message)
        with pytest.raises(BatchFlatteningUnsupportedException):
            db_sync._check_batch_flattening_supported()


class TestUpsertSourceColumns:
    def test_defaults_to_all_columns(self):
        message = _stream_schema_message({"id": {"type": ["integer"]}, "name": {"type": ["string"]}})
        db_sync = DbSync({"default_target_schema": "public"}, message)
        assert db_sync.column_order() == sorted(["id", "name"])
