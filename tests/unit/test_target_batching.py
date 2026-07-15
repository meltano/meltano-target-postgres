import json

import pytest

import target_postgres.target as target_module
from target_postgres.schema import flatten_schema


class FakeDbSync:
    """Stands in for db_sync.DbSync so batching tests never touch a real DB."""

    instances = []

    def __init__(self, config, stream_schema_message):
        self.config = config
        self.stream = stream_schema_message["stream"]
        self.key_properties = stream_schema_message.get("key_properties") or []
        self.columns = flatten_schema(
            stream_schema_message["schema"].get("properties", {}), config
        )
        self.flush_calls = []
        FakeDbSync.instances.append(self)

    def create_schema_if_not_exists(self):
        pass

    def sync_table(self):
        pass

    def primary_key_string(self, record, fallback_index):
        if not self.key_properties:
            return f"RID-{fallback_index}"
        return ",".join(str(record.get(kp)) for kp in self.key_properties)

    def flush(self, records):
        self.flush_calls.append(dict(records))
        return {"inserts": len(records), "updates": 0, "size_bytes": 0}


@pytest.fixture(autouse=True)
def patch_db_sync(monkeypatch):
    FakeDbSync.instances = []
    monkeypatch.setattr(target_module, "DbSync", FakeDbSync)
    yield


def _schema_line(stream, key_properties=("id",)):
    return json.dumps(
        {
            "type": "SCHEMA",
            "stream": stream,
            "key_properties": list(key_properties),
            "schema": {
                "properties": {
                    "id": {"type": ["integer"]},
                    "name": {"type": ["string"]},
                }
            },
        }
    )


def _record_line(stream, record_id):
    return json.dumps(
        {
            "type": "RECORD",
            "stream": stream,
            "record": {"id": record_id, "name": f"n{record_id}"},
        }
    )


def _state_line(value):
    return json.dumps({"type": "STATE", "value": value})


class TestBatching:
    def test_40_records_batch_size_20_flushes_twice(self):
        lines = [_schema_line("users")]
        lines += [_record_line("users", i) for i in range(40)]

        target_module.persist_lines({"batch_size_rows": 20}, lines)

        db_sync = FakeDbSync.instances[0]
        assert len(db_sync.flush_calls) == 2
        assert len(db_sync.flush_calls[0]) == 20
        assert len(db_sync.flush_calls[1]) == 20

    def test_fewer_records_than_batch_size_flushes_once_at_eof(self):
        lines = [_schema_line("users")]
        lines += [_record_line("users", i) for i in range(5)]

        target_module.persist_lines({"batch_size_rows": 20}, lines)

        db_sync = FakeDbSync.instances[0]
        assert len(db_sync.flush_calls) == 1
        assert len(db_sync.flush_calls[0]) == 5

    def test_duplicate_pk_collapses_within_batch(self):
        lines = [_schema_line("users")]
        lines.append(_record_line("users", 1))
        lines.append(
            _record_line("users", 1)
        )  # same PK, should collapse (last write wins)
        lines.append(_record_line("users", 2))

        target_module.persist_lines({"batch_size_rows": 20}, lines)

        db_sync = FakeDbSync.instances[0]
        assert len(db_sync.flush_calls[0]) == 2


class TestStateEmission:
    def test_flush_all_streams_false_holds_back_other_streams_bookmarks(self):
        lines = [_schema_line("a"), _schema_line("b")]
        lines.append(_state_line({"bookmarks": {"a": {"pos": 0}, "b": {"pos": 0}}}))
        # stream "a" hits batch_size_rows=2 first
        lines.append(_record_line("a", 1))
        lines.append(_record_line("a", 2))
        lines.append(_state_line({"bookmarks": {"a": {"pos": 2}, "b": {"pos": 0}}}))
        lines.append(_record_line("b", 1))

        final_state = target_module.persist_lines(
            {"batch_size_rows": 2, "flush_all_streams": False},
            lines,
        )

        # "b" never hit its batch threshold as an intermediate flush, so its
        # bookmark should reflect what was known as of the (only) "a" flush,
        # not any later state. Only "a" advances at the intermediate flush;
        # final state (after EOF flush of everything) reflects the last known
        # full state.
        assert final_state is not None
        assert final_state["bookmarks"]["a"]["pos"] == 2

    def test_flush_all_streams_true_advances_whole_state_together(self):
        lines = [_schema_line("a"), _schema_line("b")]
        lines.append(_state_line({"bookmarks": {"a": {"pos": 0}, "b": {"pos": 0}}}))
        lines.append(_record_line("a", 1))
        lines.append(_record_line("a", 2))
        lines.append(_state_line({"bookmarks": {"a": {"pos": 2}, "b": {"pos": 7}}}))
        lines.append(_record_line("b", 1))

        final_state = target_module.persist_lines(
            {"batch_size_rows": 2, "flush_all_streams": True}, lines
        )

        assert final_state is not None
        assert final_state["bookmarks"]["a"]["pos"] == 2
        assert final_state["bookmarks"]["b"]["pos"] == 7


class TestPrimaryKeyRequired:
    def test_missing_key_properties_raises_by_default(self):
        from target_postgres.exceptions import PrimaryKeyNotFoundException

        lines = [_schema_line("users", key_properties=())]
        with pytest.raises(PrimaryKeyNotFoundException):
            target_module.persist_lines({}, lines)

    def test_missing_key_properties_allowed_when_disabled(self):
        lines = [_schema_line("users", key_properties=())]
        lines.append(_record_line("users", 1))

        target_module.persist_lines({"primary_key_required": False}, lines)

        db_sync = FakeDbSync.instances[0]
        assert list(db_sync.flush_calls[0].keys()) == ["RID-0"]
