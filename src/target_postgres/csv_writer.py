"""CSV row rendering matching the original's COPY FORMAT CSV ESCAPE '\\' semantics (SPEC.md §15).

Every field is JSON-encoded (json.dumps(value, ensure_ascii=False)) except
NULL/falsy-but-not-zero values, which become empty CSV fields.

Quoting is hand-rolled (rather than the stdlib `csv` module) because
Postgres's `COPY ... WITH (FORMAT CSV, ESCAPE '\\')` dialect differs from
what Python's csv.writer produces for a non-default escapechar: Postgres
still *wraps* a field in the quote character whenever it contains the
delimiter/quote/newline, and only the quote character itself is
backslash-escaped inside that wrapping -- csv.writer with
`doublequote=False` instead skips the wrapping quotes entirely and just
backslash-escapes the quote character in place, which COPY would not parse
back correctly.
"""

import json

_NEEDS_QUOTING = (",", '"', "\n", "\r")


def _is_blank(value) -> bool:
    if isinstance(value, bool):
        return not value
    if isinstance(value, (int, float)) and value == 0:
        return False
    return not value


def _field_repr(value) -> str:
    if _is_blank(value):
        return ""
    return json.dumps(value, ensure_ascii=False)


def _csv_field(field: str) -> str:
    if any(ch in field for ch in _NEEDS_QUOTING):
        return '"' + field.replace('"', '\\"') + '"'
    return field


def record_to_csv_row(record: dict, columns: list[str]) -> str:
    """Render a single CSV row (including trailing newline) for `columns`, in order."""
    fields = (_csv_field(_field_repr(record.get(col))) for col in columns)
    return ",".join(fields) + "\n"


def records_to_csv(records: list[dict], columns: list[str]) -> str:
    """Render multiple buffered records into one CSV payload for COPY."""
    return "".join(record_to_csv_row(record, columns) for record in records)
