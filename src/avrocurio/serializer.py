"""Async Avro serializer/deserializer with Confluent Schema Registry framing."""

from io import BytesIO
from typing import TypeVar, overload

import fastavro
from dataclasses_avroschema import AvroModel
from fastavro.types import AvroMessage

from .exceptions import (
    DeserializationError,
    SchemaMatchError,
    SchemaMismatchError,
    SchemaRegistrationError,
    SerializationError,
)
from .schema_client import ApicurioClient
from .wire_format import ConfluentWireFormat

T = TypeVar("T", bound=AvroModel)


class AvroSerializer:
    """
    Async Avro serializer/deserializer using Confluent Schema Registry wire format.

    This class handles serialization and deserialization of AvroModel objects
    with Confluent Schema Registry framing and Apicurio for schema management.
    """

    def __init__(self, client: ApicurioClient) -> None:
        """
        Initialize the Avro serializer.

        Args:
            client: Apicurio client for schema operations

        """
        self.client = client

    async def serialize(self, obj: AvroModel, schema_id: int | None = None) -> bytes:
        """
        Serialize an AvroModel object with a specific schema ID or automatic lookup.

        If schema_id is provided, uses that schema directly. If schema_id is None,
        attempts to automatically find a matching schema in the registry.

        Args:
            obj: AvroModel object to serialize
            schema_id: Optional schema ID to use for encoding

        Returns:
            Avro Single Object Encoded binary with Confluent wire format framing

        Raises:
            SerializationError: If serialization fails
            SchemaMatchError: If no matching schema found during automatic lookup

        """
        try:
            if schema_id is not None:
                return await self._serialize_with_schema_id(obj, schema_id)

            schema_content = obj.avro_schema()
            match_result = await self.client.find_artifact_by_content(schema_content)
            if match_result is None:
                raise SchemaMatchError(  # noqa: TRY301
                    "No matching schema found in registry for automatic lookup. "
                    "Please register the schema manually or specify a schema_id."
                )

            # Get schema ID and serialize
            found_group_id, found_artifact_id = match_result
            found_schema_id, _ = await self.client.get_latest_schema(found_group_id, found_artifact_id)
            return await self._serialize_with_schema_id(obj, found_schema_id)

        except SchemaMatchError:
            raise
        except Exception as e:
            error_context = f"schema ID {schema_id}" if schema_id is not None else "automatic schema lookup"
            msg = f"Failed to serialize object with {error_context}: {e}"
            raise SerializationError(msg) from e

    async def _serialize_with_schema_id(self, obj: AvroModel, schema_id: int) -> bytes:
        """
        Core serialization logic using a specific schema ID.

        Args:
            obj: AvroModel object to serialize
            schema_id: Schema ID to use for encoding

        Returns:
            Avro Single Object Encoded binary with Confluent wire format framing

        Raises:
            SerializationError: If serialization fails
            SchemaMismatchError: If object doesn't match the schema

        """
        schema = await self.client.get_schema_by_global_id(schema_id)
        obj_dict = obj.asdict()

        buffer = BytesIO()
        try:
            fastavro.schemaless_writer(buffer, schema, obj_dict)
        except Exception as e:
            msg = f"Data does not match schema: {e}"
            raise SchemaMismatchError(msg) from e
        avro_payload = buffer.getvalue()

        return ConfluentWireFormat.encode(schema_id, avro_payload)

    @overload
    async def deserialize(self, message: bytes, model: type[T]) -> T: ...

    @overload
    async def deserialize(self, message: bytes, model: None = None) -> AvroMessage: ...

    async def deserialize(self, message: bytes, model: type[T] | None = None) -> T | AvroMessage:
        """
        Deserialize a message to an AvroModel object or, when target_class is not given, an AvroMessage.

        Args:
            message: Serialized message with Confluent wire format framing
            model: AvroModel class to deserialize into, or None to just get the raw AvroMessage.

        Returns:
            Deserialized AvroModel object if target_class provided, otherwise a raw AvroMessage.

        Raises:
            DeserializationError: If deserialization fails

        """
        try:
            schema_id, avro_payload = ConfluentWireFormat.decode(message)
            schema = await self.client.get_schema_by_global_id(schema_id)
            buffer = BytesIO(avro_payload)

            if model is None:
                return fastavro.schemaless_reader(buffer, writer_schema=schema, reader_schema=None)

            obj_dict = fastavro.schemaless_reader(
                buffer,
                writer_schema=schema,
                reader_schema=model.avro_schema_to_python(),
            )
            return model.parse_obj(obj_dict)  # type: ignore[return-value]

        except DeserializationError:
            raise
        except Exception as e:
            msg = f"Failed to deserialize message: {e}"
            raise DeserializationError(msg) from e

    async def register_schema(self, obj: AvroModel, group: str, artifact_name: str) -> int:
        """
        Register a schema derived from an AvroModel object.

        Args:
            obj: AvroModel object to derive schema from
            group: Group ID to register schema under
            artifact_name: Artifact ID to register schema under

        Returns:
            Global ID of the registered schema

        Raises:
            SchemaRegistrationError: If schema registration fails

        """
        try:
            schema_content = obj.avro_schema()
            return await self.client.register_schema(
                group=group, artifact_name=artifact_name, schema_content=schema_content
            )
        except Exception as e:
            raise SchemaRegistrationError(group=group, artifact_name=artifact_name) from e
