"""AvroCurio: Apache Avro serialization with Confluent Schema Registry framing and Apicurio integration."""

from .serializer import AvroSerializer
from .schema_client import ApicurioClient
from .config import ApicurioConfig
from .exceptions import (
    AvroCurioError,
    SchemaNotFoundError,
    InvalidWireFormatError,
    SerializationError,
    DeserializationError,
    SchemaMismatchError,
    SchemaMatchError,
)

__all__ = [
    "AvroSerializer",
    "ApicurioClient", 
    "ApicurioConfig",
    "AvroCurioError",
    "SchemaNotFoundError",
    "InvalidWireFormatError",
    "SerializationError",
    "DeserializationError", 
    "SchemaMismatchError",
    "SchemaMatchError",
    "create_serializer",
]

__version__ = "0.1.0"


async def create_serializer(config: ApicurioConfig) -> AvroSerializer:
    """Convenience function to create an AvroSerializer with an ApicurioClient.
    
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
