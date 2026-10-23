"""Arrow / ADBC connectivity for Singer BATCH messages with encoding.format == "arrow".

``pyarrow`` and ``adbc-driver-postgresql`` are regular (required) dependencies of this
package - all three are pure-wheel/pip-installable, unlike some other ADBC drivers (e.g. MySQL's)
that require a separate native driver install step.

Nothing in this module is imported eagerly by target.py/db_sync.py; it is only
touched when a BATCH message with encoding.format == "arrow" actually arrives, so a
broken/partial install of these packages never breaks RECORD-only syncs.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from urllib.parse import quote_plus

from target_postgres.exceptions import ArrowValueError

if TYPE_CHECKING:
    from pyarrow import Table


def strip_file_uri(manifest: list) -> list:
    """Strip the `file://` URI scheme off each entry in a BATCH message's manifest,
    returning plain local filesystem paths."""
    return [path[len("file://") :] if path.startswith("file://") else path for path in manifest]


def _coerce_types(table: Table) -> Table:
    import pyarrow as pa
    import pyarrow.compute as pc

    for i, field in enumerate(table.schema):
        if pa.types.is_decimal(field.type):
            # Not every decimal bit-width Arrow supports has a corresponding staging-table type it knows how
            # to create. Casting to string sidesteps that entirely (and, unlike a float64 cast,
            # loses no precision); DbSync._upsert already casts every staging-sourced column to
            # its real target type (from the SCHEMA message) on the INSERT ... SELECT that follows,
            # so a string-typed staging column is no different from any other Arrow-native type
            # mismatch it already handles.
            table = table.set_column(i, field.name, pc.cast(table.column(i), pa.string()))
        elif pa.types.is_struct(field.type) or pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            # A struct or list column comes from an `object` or `array` property, which the SCHEMA message types as
            # jsonb, and `DbSync._upsert` already casts every staging-sourced column to its real target type, so a
            # JSON string is all the staging table needs to carry. ADBC can't create a staging column for a struct,
            # and while it can for a list (e.g. `integer[]`), Postgres has no cast from an array to jsonb. A null
            # struct or list stays a SQL NULL.
            try:
                values = [
                    None if val is None else json.dumps(val, default=str, allow_nan=False)
                    for val in table.column(i).to_pylist()
                ]
            except ValueError as err:
                msg = f"Column '{field.name}' of type `{field.type!s}` has an unsupported value"
                raise ArrowValueError(msg) from err
            table = table.set_column(i, field.name, pa.array(values, type=pa.string()))

    return table


def read_manifest_tables(file_paths: list):
    """Read one or more Arrow IPC file-format files into a single combined pyarrow.Table.

    Does not delete the source files -- callers should only do that after the full
    load succeeds, so a failure partway through a multi-file batch leaves every
    source file intact for a retry.
    """
    import pyarrow as pa
    import pyarrow.ipc as ipc

    tables = []
    for file_path in file_paths:
        with ipc.open_file(file_path) as reader:
            tables.append(reader.read_all())

    return _coerce_types(pa.concat_tables(tables, promote_options="default"))


def build_adbc_uri(config: dict) -> str:
    """Build a libpq-style URI for adbc_driver_postgresql.dbapi.connect() from config.

    Mirrors DbSync._connect_kwargs, but as a URI string since ADBC's Postgres driver
    takes a connection URI rather than keyword arguments.
    """
    user = quote_plus(str(config["user"]))
    password = quote_plus(str(config["password"]))
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]

    uri = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    if config.get("ssl") is True:
        uri += "?sslmode=require"
    return uri
