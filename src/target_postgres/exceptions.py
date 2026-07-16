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


class UnsupportedBatchEncodingException(TargetPostgresException):
    """Raised when a BATCH message's encoding.format is anything other than 'arrow'."""


class BatchFlatteningUnsupportedException(TargetPostgresException):
    """Raised when a stream's flattening/inflection config would rename columns away from
    the raw schema property names that a BATCH-sourced Arrow file uses (SPEC.md-style
    by-name column matching can't bridge that gap)."""
