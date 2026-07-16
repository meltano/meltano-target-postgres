"""Singer message ingestion loop: SCHEMA/RECORD/STATE/ACTIVATE_VERSION dispatch,
per-stream batching, and flush scheduling (SPEC.md §7, §8, §9).
"""

import json
import sys
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, InvalidOperation

from jsonschema import Draft7Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from target_postgres.arrow_batch import strip_file_uri
from target_postgres.db_sync import DbSync
from target_postgres.exceptions import (
    InvalidValidationOperationException,
    PrimaryKeyNotFoundException,
    RecordValidationException,
    UnsupportedBatchEncodingException,
)
from target_postgres.logger import Counter
from target_postgres.schema import add_metadata_values_to_record, flatten_record

SUPPORTED_BATCH_ENCODING_FORMAT = "arrow"


class _StreamState:
    def __init__(self, db_sync: DbSync, validator):
        self.db_sync = db_sync
        self.validator = validator
        self.buffer: dict = {}
        self.row_count = 0


def _floats_to_decimal(value):
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _floats_to_decimal(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_floats_to_decimal(v) for v in value]
    return value


def _validate_record(validator: Draft7Validator, record: dict):
    converted = _floats_to_decimal(record)
    try:
        validator.validate(converted)
    except InvalidOperation as exc:
        raise InvalidValidationOperationException(str(exc)) from exc
    except ValidationError as exc:
        raise RecordValidationException(str(exc)) from exc


def _parallelism(config: dict, n_streams: int) -> int:
    parallelism = config.get("parallelism", 0)
    if parallelism and parallelism > 0:
        return parallelism
    max_parallelism = config.get("max_parallelism", 16)
    return max(1, min(n_streams, max_parallelism))


def _flush_one(stream_state: _StreamState):
    with Counter("record_count", {"stream": stream_state.db_sync.stream}) as counter:
        result = stream_state.db_sync.flush(stream_state.buffer)
        counter.increment(result["inserts"] + result["updates"])
    stream_state.buffer = {}


def _load_batch(stream_state: _StreamState, file_paths: list):
    with Counter("record_count", {"stream": stream_state.db_sync.stream}) as counter:
        result = stream_state.db_sync.load_rows_from_arrow_files(file_paths)
        counter.increment(result["inserts"] + result["updates"])


def _merge_flushed_state(flushed_state, current_state, flushed_stream_names: list):
    """Advance only the flushed streams' bookmarks, holding others back (SPEC.md §9)."""
    if current_state is None:
        return flushed_state
    if not current_state.get("bookmarks"):
        return current_state

    merged_bookmarks = dict((flushed_state or {}).get("bookmarks", {}))
    for name in flushed_stream_names:
        if name in current_state["bookmarks"]:
            merged_bookmarks[name] = current_state["bookmarks"][name]

    merged = dict(current_state)
    merged["bookmarks"] = merged_bookmarks
    return merged


def _flush_streams(target_streams: dict, state, flushed_state, config: dict, flush_all: bool = False):
    if not target_streams:
        return flushed_state

    workers = _parallelism(config, len(target_streams))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_flush_one, stream_state) for stream_state in target_streams.values()]
        for future in futures:
            future.result()

    if flush_all:
        return state
    return _merge_flushed_state(flushed_state, state, list(target_streams.keys()))


def _emit_state(state):
    if state is None:
        return
    sys.stdout.write(json.dumps(state) + "\n")
    sys.stdout.flush()


def persist_lines(config: dict, lines) -> dict | None:
    """Consume an iterable of Singer message lines, returning the final flushed state."""
    streams: dict[str, _StreamState] = {}
    state = None
    flushed_state = None

    batch_size_rows = config.get("batch_size_rows", 100000)
    flush_all_streams = config.get("flush_all_streams", False)
    primary_key_required = config.get("primary_key_required", True)
    validate_records = config.get("validate_records", False)
    add_metadata_columns = config.get("add_metadata_columns", False)
    hard_delete = config.get("hard_delete", False)

    for line in lines:
        if not line.strip():
            continue
        message = json.loads(line)
        msg_type = message.get("type")

        if msg_type == "SCHEMA":
            stream = message["stream"]
            key_properties = message.get("key_properties") or []
            if primary_key_required and not key_properties:
                raise PrimaryKeyNotFoundException(
                    f"Stream {stream!r} has no key_properties and primary_key_required is set"
                )

            if stream in streams and streams[stream].buffer:
                flushed_state = _flush_streams({stream: streams[stream]}, state, flushed_state, config)

            db_sync = DbSync(
                config,
                {
                    "stream": stream,
                    "key_properties": key_properties,
                    "schema": message["schema"],
                },
            )
            db_sync.create_schema_if_not_exists()
            db_sync.sync_table()

            validator = None
            if validate_records:
                validator = Draft7Validator(message["schema"], format_checker=FormatChecker())

            streams[stream] = _StreamState(db_sync, validator)

        elif msg_type == "RECORD":
            stream = message["stream"]
            if stream not in streams:
                raise Exception(f"RECORD message for stream {stream!r} received before its SCHEMA")

            stream_state = streams[stream]
            record = message["record"]

            if stream_state.validator is not None:
                _validate_record(stream_state.validator, record)

            if add_metadata_columns or hard_delete:
                record = add_metadata_values_to_record(record, message.get("time_extracted"))

            pk_string = stream_state.db_sync.primary_key_string(record, stream_state.row_count)
            stream_state.buffer[pk_string] = flatten_record(record, stream_state.db_sync.columns)
            stream_state.row_count += 1

            if len(stream_state.buffer) >= batch_size_rows:
                if flush_all_streams:
                    flushed_state = _flush_streams(streams, state, flushed_state, config, flush_all=True)
                else:
                    flushed_state = _flush_streams({stream: stream_state}, state, flushed_state, config)
                _emit_state(flushed_state)

        elif msg_type == "BATCH":
            stream = message["stream"]
            if stream not in streams:
                raise Exception(f"BATCH message for stream {stream!r} received before its SCHEMA")

            encoding = message.get("encoding", {})
            if encoding.get("format") != SUPPORTED_BATCH_ENCODING_FORMAT:
                raise UnsupportedBatchEncodingException(
                    f"Unsupported BATCH encoding format {encoding.get('format')!r} for "
                    f"stream {stream!r} (only {SUPPORTED_BATCH_ENCODING_FORMAT!r} is supported)"
                )

            stream_state = streams[stream]
            if stream_state.buffer:
                flushed_state = _flush_streams({stream: stream_state}, state, flushed_state, config)

            file_paths = strip_file_uri(message.get("manifest", []))
            _load_batch(stream_state, file_paths)

        elif msg_type == "STATE":
            state = message["value"]
            if flushed_state is None:
                flushed_state = state

        elif msg_type == "ACTIVATE_VERSION":
            if flushed_state is None:
                flushed_state = state

        else:
            raise Exception(f"Unknown Singer message type: {msg_type!r}")

    pending = {name: stream_state for name, stream_state in streams.items() if stream_state.buffer}
    if pending:
        flushed_state = _flush_streams(pending, state, flushed_state, config, flush_all=True)

    _emit_state(flushed_state)
    return flushed_state
