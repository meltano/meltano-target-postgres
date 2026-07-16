"""Stream/schema/table/column naming rules (SPEC.md §5, §6).

Column/table identifiers themselves are quoted at the SQL-building layer
(db_sync.py, via psycopg.sql.Identifier) -- this module only computes the
normalized, deduplicated, length-safe *names*.
"""

import re
import uuid

from target_postgres.exceptions import TargetSchemaNotFoundException

MAX_IDENTIFIER_LENGTH = 63
FLATTEN_SEPARATOR = "__"


def underscore(word: str) -> str:
    """Equivalent of inflection.underscore() for the ~2 cases this target needs."""
    word = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", word)
    word = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", word)
    word = word.replace("-", "_")
    return word.lower()


def inflect_name(name: str) -> str:
    """camelCase/PascalCase -> snake_case, collapsing runs of capitals specially.

    Two pre-passes insert an extra underscore before the standard underscore()
    split so that e.g. "HTTPHeader_Value" -> "http_header__value" instead of
    "http_header_value".
    """
    name = re.sub(r"([A-Z]+)_([A-Z][a-z])", r"\1__\2", name)
    name = re.sub(r"([a-z0-9])_([A-Z])", r"\1__\2", name)
    return underscore(name)


def abbreviate(segment: str) -> str:
    """Camelize a path segment then keep only the uppercase "initials".

    Falls back to the first 3 characters if that yields <=1 char.
    """
    camelized = "".join(word[:1].upper() + word[1:] for word in re.split(r"[_\-\s]+", segment) if word)
    initials = "".join(c for c in camelized if c.isupper())
    if len(initials) <= 1:
        return segment[:3]
    return initials


def flatten_key(key_parts: list[str], sep: str = FLATTEN_SEPARATOR) -> str:
    """Join key_parts with sep, abbreviating earlier segments until <=63 chars."""
    parts = list(key_parts)
    name = sep.join(parts)
    if len(name) < MAX_IDENTIFIER_LENGTH:
        return name

    for i in range(len(parts) - 1):
        parts[i] = abbreviate(parts[i])
        name = sep.join(parts)
        if len(name) < MAX_IDENTIFIER_LENGTH:
            return name

    return name[:MAX_IDENTIFIER_LENGTH]


def deduplicate_name(name: str, seen: dict[str, int]) -> str:
    """Resolve a collision against previously-seen names, appending __1, __2, ...

    `seen` is shared/mutated across the whole flatten call, so callers must
    process names in sorted-key order for deterministic results (SPEC.md §5).
    """
    if name not in seen:
        seen[name] = 0
        return name

    seen[name] += 1
    suffix = f"{FLATTEN_SEPARATOR}{seen[name]}"
    truncated = name[: MAX_IDENTIFIER_LENGTH - len(suffix)] + suffix
    while truncated in seen:
        seen[name] += 1
        suffix = f"{FLATTEN_SEPARATOR}{seen[name]}"
        truncated = name[: MAX_IDENTIFIER_LENGTH - len(suffix)] + suffix
    seen[truncated] = 0
    return truncated


def stream_name_to_dict(stream_name: str, separator: str = "-") -> dict:
    """Split a stream-id into catalog/schema/table segments.

    <table>                        -> table only
    <schema>-<table>                -> schema + table
    <catalog>-<schema>-<table...>   -> catalog + schema + table (extra segments
                                        after the 2nd are rejoined with '_')
    """
    segments = stream_name.split(separator)

    if len(segments) == 1:
        return {"catalog_name": None, "schema_name": None, "table_name": segments[0]}
    if len(segments) == 2:
        return {
            "catalog_name": None,
            "schema_name": segments[0],
            "table_name": segments[1],
        }

    return {
        "catalog_name": segments[0],
        "schema_name": segments[1],
        "table_name": "_".join(segments[2:]),
    }


def resolve_target_schema(stream_schema_name: str | None, config: dict) -> str:
    """Resolve the Postgres target schema for a stream (SPEC.md §6)."""
    schema_mapping = config.get("schema_mapping") or {}
    if stream_schema_name and stream_schema_name in schema_mapping:
        return schema_mapping[stream_schema_name]["target_schema"]

    default_target_schema = config.get("default_target_schema")
    if default_target_schema:
        return default_target_schema

    raise TargetSchemaNotFoundException(
        f"Cannot resolve target schema for stream schema segment {stream_schema_name!r}: "
        "no matching schema_mapping entry and no default_target_schema configured"
    )


def resolve_grantees(stream_schema_name: str | None, config: dict):
    """Resolve select-permission grantees for a stream (SPEC.md §6)."""
    schema_mapping = config.get("schema_mapping") or {}
    if stream_schema_name and stream_schema_name in schema_mapping:
        grantees = schema_mapping[stream_schema_name].get("target_schema_select_permissions")
        if grantees:
            return grantees

    return config.get("default_target_schema_select_permissions")


def resolve_indices(stream_schema_name: str | None, table: str, config: dict) -> list[str]:
    """Resolve configured index columns for a stream's table (SPEC.md §11)."""
    schema_mapping = config.get("schema_mapping") or {}
    if stream_schema_name and stream_schema_name in schema_mapping:
        indices = schema_mapping[stream_schema_name].get("indices") or {}
        return indices.get(table, [])
    return []


def safe_table_name(raw_table: str) -> str:
    """Normalize a stream's table segment into a Postgres-safe table name."""
    return raw_table.replace(".", "_").replace("-", "_").lower()


def temp_table_name() -> str:
    return "tmp_" + str(uuid.uuid4()).replace("-", "_")
