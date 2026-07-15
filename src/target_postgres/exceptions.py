"""Exceptions raised while ingesting Singer messages."""


class TargetPostgresException(Exception):
    """Base class for all target-postgres errors."""


class PrimaryKeyNotFoundException(TargetPostgresException):
    """Raised when a SCHEMA message has no key_properties and primary_key_required is set."""


class TargetSchemaNotFoundException(TargetPostgresException):
    """Raised when a stream's target schema cannot be resolved from config."""


class RecordValidationException(TargetPostgresException):
    """Raised when a RECORD fails JSON Schema validation."""


class InvalidValidationOperationException(TargetPostgresException):
    """Raised when record validation itself fails with a decimal.InvalidOperation."""
