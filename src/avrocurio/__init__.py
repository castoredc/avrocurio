"""AvroCurio: Apache Avro serialization with Confluent Schema Registry framing and Apicurio integration."""

from .config import ApicurioConfig
from .exceptions import (
    AvroCurioError,
    DeserializationError,
    InvalidWireFormatError,
    SchemaMatchError,
    SchemaMismatchError,
    SchemaNotFoundError,
    SerializationError,
)
from .schema_client import ApicurioClient
from .serializer import AvroSerializer

__all__ = [
    "ApicurioClient",
    "ApicurioConfig",
    "AvroCurioError",
    "AvroSerializer",
    "DeserializationError",
    "InvalidWireFormatError",
    "SchemaMatchError",
    "SchemaMismatchError",
    "SchemaNotFoundError",
    "SerializationError",
    "create_serializer",
]

__version__ = "0.1.0"


async def create_serializer(config: ApicurioConfig) -> AvroSerializer:
    """
    Create an AvroSerializer with an ApicurioClient.

    Args:
        config: Configuration for the Apicurio Registry connection

    Returns:
        Configured AvroSerializer instance

    Note:
        The returned serializer uses a client that should be properly closed
        when done. Consider using the ApicurioClient as an async context manager.

    """
    client = ApicurioClient(config)
    return AvroSerializer(client)
