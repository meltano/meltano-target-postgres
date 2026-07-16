"""Arrow / ADBC connectivity for Singer BATCH messages with encoding.format == "arrow".

``pyarrow`` and ``adbc-driver-postgresql`` are regular (required) dependencies of this
package - all three are pure-wheel/pip-installable, unlike some other ADBC drivers (e.g. MySQL's)
that require a separate native driver install step.

Nothing in this module is imported eagerly by target.py/db_sync.py; it is only
touched when a BATCH message with encoding.format == "arrow" actually arrives, so a
broken/partial install of these packages never breaks RECORD-only syncs.
"""

from __future__ import annotations

from urllib.parse import quote_plus


def strip_file_uri(manifest: list) -> list:
    """Strip the `file://` URI scheme off each entry in a BATCH message's manifest,
    returning plain local filesystem paths."""
    return [path[len("file://") :] if path.startswith("file://") else path for path in manifest]


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

    return pa.concat_tables(tables, promote_options="default")


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
