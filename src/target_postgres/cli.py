"""CLI entry point: `target-postgres --config config.json`, reading Singer messages from stdin."""

import argparse
import json
import sys

from target_postgres.config import validate_config
from target_postgres.logger import LOGGER
from target_postgres.target import persist_lines


def main() -> None:
    parser = argparse.ArgumentParser(prog="target-postgres")
    parser.add_argument("-c", "--config", help="Config file", default=None)
    args = parser.parse_args()

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
