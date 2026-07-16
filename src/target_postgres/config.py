"""Config validation.

Per SPEC.md §3: this only checks that the 5 connection keys are non-empty and
that at least one of default_target_schema / schema_mapping is set. It does
not validate types, ranges, or connectivity -- invalid combinations fail later
against Postgres itself.
"""

REQUIRED_CONNECTION_KEYS = ("host", "port", "user", "password", "dbname")


def validate_config(config: dict) -> list[str]:
    """Return a list of human-readable config errors (empty if config is valid)."""
    errors = []

    for key in REQUIRED_CONNECTION_KEYS:
        if not config.get(key):
            errors.append(f"Missing required config key: {key!r}")

    if not config.get("default_target_schema") and not config.get("schema_mapping"):
        errors.append("Config must set either 'default_target_schema' or 'schema_mapping'")

    return errors
