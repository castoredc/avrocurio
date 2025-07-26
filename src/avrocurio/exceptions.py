"""Custom exceptions for AvroCurio library."""


class AvroCurioError(Exception):
    """Base exception for all AvroCurio errors."""
    pass


class SchemaNotFoundError(AvroCurioError):
    """Raised when a schema cannot be found in the registry."""
    pass


class InvalidWireFormatError(AvroCurioError):
    """Raised when the message does not follow Confluent wire format."""
    pass


class SerializationError(AvroCurioError):
    """Raised when serialization fails."""
    pass


class DeserializationError(AvroCurioError):
    """Raised when deserialization fails."""
    pass


class SchemaMismatchError(AvroCurioError):
    """Raised when schema validation fails."""
    pass


class SchemaMatchError(AvroCurioError):
    """Raised when no matching schema is found in the registry for automatic lookup."""
    pass