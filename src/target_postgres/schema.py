"""JSON Schema -> Postgres column type mapping and schema/record flattening (SPEC.md §4, §5, §8)."""

from datetime import datetime

from target_postgres.naming import deduplicate_name, flatten_key, inflect_name

METADATA_COLUMNS = {
    "_sdc_extracted_at": {"type": ["null", "string"], "format": "date-time"},
    "_sdc_batched_at": {"type": ["null", "string"], "format": "date-time"},
    "_sdc_deleted_at": {"type": ["null", "string"]},
}


def _types_of(prop_schema: dict) -> set:
    prop_type = prop_schema.get("type", []) if isinstance(prop_schema, dict) else []
    if isinstance(prop_type, str):
        prop_type = [prop_type]
    return {t for t in prop_type if t != "null"}


def column_type(prop_schema: dict) -> str:
    """Map a single flattened property's JSON Schema to a Postgres column type (SPEC.md §4)."""
    types = _types_of(prop_schema)
    prop_format = prop_schema.get("format")
    maximum = prop_schema.get("maximum")

    if "object" in types or "array" in types:
        return "jsonb"
    if prop_format == "date-time":
        return "timestamp without time zone"
    if prop_format == "time":
        return "time without time zone"
    if prop_format == "date":
        return "date"
    if "number" in types:
        return "double precision"
    if "integer" in types and "string" in types:
        return "character varying"
    if "integer" in types:
        if maximum is not None:
            if maximum <= 32767:
                return "smallint"
            if maximum <= 2147483647:
                return "integer"
            if maximum <= 9223372036854775807:
                return "bigint"
            # SPEC.md §4 point 6: original falls through with no type assigned here
            # (a bug); fixed in this reimplementation to fall back to numeric.
            return "numeric"
        return "numeric"
    if "boolean" in types:
        return "boolean"
    return "character varying"


def add_metadata_columns_to_schema(properties: dict) -> dict:
    """Inject the 3 _sdc_* metadata properties into a stream's schema properties."""
    merged = dict(properties)
    merged.update(METADATA_COLUMNS)
    return merged


def add_metadata_values_to_record(record: dict, time_extracted: str | None) -> dict:
    """Inject the 3 _sdc_* metadata values into a RECORD's data dict (SPEC.md §8)."""
    merged = dict(record)
    merged["_sdc_extracted_at"] = time_extracted
    merged["_sdc_batched_at"] = datetime.now().isoformat()
    merged["_sdc_deleted_at"] = record.get("_sdc_deleted_at")
    return merged


def _collect_leaves(
    properties: dict,
    max_level: int,
    inflect: bool,
    orig_parts: list | None = None,
    key_parts: list | None = None,
    level: int = 0,
) -> list[tuple[list, list, dict]]:
    orig_parts = orig_parts or []
    key_parts = key_parts or []
    leaves = []

    for name, prop_schema in properties.items():
        name_for_column = inflect_name(name) if inflect else name
        new_orig = orig_parts + [name]
        new_key = key_parts + [name_for_column]

        nested_properties = prop_schema.get("properties") if isinstance(prop_schema, dict) else None
        types = _types_of(prop_schema)

        if level < max_level and "object" in types and nested_properties:
            leaves.extend(_collect_leaves(nested_properties, max_level, inflect, new_orig, new_key, level + 1))
        else:
            leaves.append((new_orig, new_key, prop_schema))

    return leaves


def _flattened_columns(properties: dict, config: dict) -> list[dict]:
    """Returns an ordered list of {"path": [...], "name": str, "schema": {...}}.

    Dedup/truncation resolution order is by sorted *flattened* column name
    (SPEC.md §5), which is deterministic regardless of input property order.
    """
    max_level = config.get("data_flattening_max_level", 0)
    inflect = config.get("underscore_camel_case_fields", False)

    leaves = _collect_leaves(properties, max_level, inflect)
    ordered = sorted(leaves, key=lambda leaf: flatten_key(leaf[1]))

    seen: dict[str, int] = {}
    columns = []
    for orig_parts, key_parts, prop_schema in ordered:
        base_name = flatten_key(key_parts)
        final_name = deduplicate_name(base_name, seen)
        columns.append({"path": orig_parts, "name": final_name, "schema": prop_schema})

    return columns


def flatten_schema(properties: dict, config: dict) -> list[dict]:
    """Flatten a stream's JSON Schema properties into Postgres columns (SPEC.md §5)."""
    return _flattened_columns(properties, config)


def _get_by_path(record: dict, path: list):
    value = record
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return None, False
        value = value[key]
    return value, True


def flatten_record(record: dict, columns: list[dict]) -> dict:
    """Flatten a RECORD's data using the same column layout computed by flatten_schema.

    Dict/list values are passed through as-is; the CSV-writing layer
    (csv_writer.py) JSON-encodes every field uniformly, which is what
    ultimately produces valid JSON text for jsonb columns (SPEC.md §5, §15).
    """
    result = {}
    for col in columns:
        value, present = _get_by_path(record, col["path"])
        if present:
            result[col["name"]] = value
    return result
