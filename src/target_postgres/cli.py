"""CLI entry point: `target-postgres --config config.json`, reading Singer messages from stdin."""

import argparse
import json
import sys
from importlib.metadata import PackageNotFoundError, version

from target_postgres.config import validate_config
from target_postgres.logger import LOGGER
from target_postgres.target import persist_lines

CAPABILITIES = [
    "about",
    "target-schema",
    "hard-delete",
    "validate-records",
    "schema-flattening",
    "batch",
]


def about_info() -> dict:
    try:
        target_version = version("target-postgres")
    except PackageNotFoundError:
        target_version = "unknown"

    return {
        "name": "target-postgres",
        "description": "Singer target for loading data into PostgreSQL",
        "version": target_version,
        "capabilities": CAPABILITIES,
    }


def main() -> None:
    parser = argparse.ArgumentParser(prog="target-postgres")
    parser.add_argument("-c", "--config", help="Config file", default=None)
    parser.add_argument("--about", action="store_true", help="Show information about this plugin and exit")
    args = parser.parse_args()

    if args.about:
        print(json.dumps(about_info()))
        return

    if args.config:
        with open(args.config, encoding="utf-8") as config_file:
            config = json.load(config_file)
    else:
        config = {}

    errors = validate_config(config)
    if errors:
        for error in errors:
            LOGGER.error(error)
        sys.exit(1)

    persist_lines(config, sys.stdin)


if __name__ == "__main__":
    main()
