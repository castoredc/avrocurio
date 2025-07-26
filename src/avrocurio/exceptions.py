"""Custom exceptions for AvroCurio library."""


class AvroCurioError(Exception):
    """Base exception for all AvroCurio errors."""


class SchemaNotFoundError(AvroCurioError):
    """Raised when a schema cannot be found in the registry."""


class InvalidWireFormatError(AvroCurioError):
    """Raised when the message does not follow Confluent wire format."""


class SerializationError(AvroCurioError):
    """Raised when serialization fails."""


class DeserializationError(AvroCurioError):
    """Raised when deserialization fails."""


class SchemaMismatchError(AvroCurioError):
    """Raised when schema validation fails."""


class SchemaMatchError(AvroCurioError):
    """Raised when no matching schema is found in the registry for automatic lookup."""


class SchemaRegistrationError(AvroCurioError):
    """Raised when schema registration fails."""

    def __init__(self, group: str, artifact_name: str) -> None:
        super().__init__(group, artifact_name)
        self.group = group
        self.artifact_name = artifact_name

    def __str__(self) -> str:
        msg = f"Failed to register schema for {self.group}/{self.artifact_name}"
        if self.__cause__ is not None:
            msg += f": {self.__cause__}"
        return msg
