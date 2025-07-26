"""Async Avro serializer/deserializer with Confluent Schema Registry framing."""

import json
from io import BytesIO
from typing import Any, Optional, Type, TypeVar

import fastavro
from dataclasses_avroschema import AvroModel

from .exceptions import (
    DeserializationError,
    SchemaMatchError,
    SchemaMismatchError,
    SerializationError,
)
from .schema_client import ApicurioClient
from .wire_format import ConfluentWireFormat

T = TypeVar("T", bound=AvroModel)


class AvroSerializer:
    """Async Avro serializer/deserializer using Confluent Schema Registry wire format.

    This class handles serialization and deserialization of AvroModel objects
    with Confluent Schema Registry framing and Apicurio for schema management.
    """

    def __init__(self, client: ApicurioClient, cache_schemas: bool = True):
        """Initialize the Avro serializer.

        Args:
            client: Apicurio client for schema operations
            cache_schemas: Whether to cache parsed schemas for performance
        """
        self.client = client
        self.cache_schemas = cache_schemas
        self._parsed_schema_cache: dict[int, dict[str, Any]] = {}

    async def serialize(self, obj: AvroModel, schema_id: Optional[int] = None) -> bytes:
        """Serialize an AvroModel object with a specific schema ID or automatic lookup.

        If schema_id is provided, uses that schema directly. If schema_id is None,
        attempts to automatically find a matching schema in the registry.

        Args:
            obj: AvroModel object to serialize
            schema_id: Optional schema ID to use for wire format framing

        Returns:
            Serialized bytes with Confluent wire format framing

        Raises:
            SerializationError: If serialization fails
            SchemaMismatchError: If object doesn't match the schema
            SchemaMatchError: If no matching schema found during automatic lookup
        """
        try:
            if schema_id is not None:
                # Traditional explicit approach
                return await self._serialize_with_schema_id(obj, schema_id)

            # NEW: Automatic lookup when schema_id is None
            schema_content = obj.avro_schema()

            # Try content search first (registry-wide)
            match_result = await self.client.find_artifact_by_content(schema_content)

            # Fallback to fingerprint search
            if match_result is None:
                match_result = await self.client.find_artifact_by_fingerprint(
                    schema_content
                )

            if match_result is None:
                raise SchemaMatchError(
                    "No matching schema found in registry for automatic lookup. "
                    "Please register the schema manually or specify a schema_id."
                )

            # Get schema ID and serialize
            found_group_id, found_artifact_id = match_result
            found_schema_id, _ = await self.client.get_latest_schema(
                found_group_id, found_artifact_id
            )
            return await self._serialize_with_schema_id(obj, found_schema_id)

        except Exception as e:
            if isinstance(
                e, (SerializationError, SchemaMismatchError, SchemaMatchError)
            ):
                raise

            # Build error message based on context
            if schema_id is not None:
                error_context = f"schema ID {schema_id}"
            else:
                error_context = "automatic schema lookup"

            raise SerializationError(
                f"Failed to serialize object with {error_context}: {e}"
            )

    async def _serialize_with_schema_id(self, obj: AvroModel, schema_id: int) -> bytes:
        """Core serialization logic using a specific schema ID.

        Args:
            obj: AvroModel object to serialize
            schema_id: Schema ID to use for wire format framing

        Returns:
            Serialized bytes with Confluent wire format framing

        Raises:
            SerializationError: If serialization fails
            SchemaMismatchError: If object doesn't match the schema
        """
        # Get the schema from registry to validate against
        registry_schema = await self.client.get_schema_by_global_id(schema_id)
        parsed_schema = await self._get_parsed_schema(schema_id, registry_schema)

        # Convert AvroModel object to dictionary
        obj_dict = obj.asdict()

        # Validate against schema
        self._validate_data_against_schema(obj_dict, parsed_schema)

        # Serialize with fastavro
        buffer = BytesIO()
        fastavro.schemaless_writer(buffer, parsed_schema, obj_dict)
        avro_payload = buffer.getvalue()

        # Apply Confluent wire format framing
        return ConfluentWireFormat.encode(schema_id, avro_payload)

    async def deserialize(self, message: bytes, target_class: Type[T]) -> T:
        """Deserialize a message to an AvroModel object.

        Args:
            message: Serialized message with Confluent wire format framing
            target_class: AvroModel class to deserialize into

        Returns:
            Deserialized AvroModel object

        Raises:
            DeserializationError: If deserialization fails
        """
        try:
            # Decode wire format to get schema ID and payload
            schema_id, avro_payload = ConfluentWireFormat.decode(message)

            # Get schema from registry
            registry_schema = await self.client.get_schema_by_global_id(schema_id)
            parsed_schema = await self._get_parsed_schema(schema_id, registry_schema)

            # Deserialize with fastavro
            buffer = BytesIO(avro_payload)
            # import ipdb

            # ipdb.set_trace()
            obj_dict = fastavro.schemaless_reader(
                buffer,
                writer_schema=parsed_schema,
                reader_schema=target_class.avro_schema_to_python(),
            )

            # Convert dictionary to AvroModel object
            return target_class.parse_obj(obj_dict)

        except Exception as e:
            if isinstance(e, DeserializationError):
                raise
            raise DeserializationError(f"Failed to deserialize message: {e}")

    async def deserialize_with_schema(
        self, message: bytes, target_class: Type[T]
    ) -> tuple[T, dict[str, Any]]:
        """Deserialize a message and return both the object and schema.

        Args:
            message: Serialized message with Confluent wire format framing
            target_class: AvroModel class to deserialize into

        Returns:
            Tuple of (deserialized_object, schema_dict)

        Raises:
            DeserializationError: If deserialization fails
        """
        try:
            # Decode wire format to get schema ID and payload
            schema_id, avro_payload = ConfluentWireFormat.decode(message)

            # Get schema from registry
            registry_schema = await self.client.get_schema_by_global_id(schema_id)
            parsed_schema = await self._get_parsed_schema(schema_id, registry_schema)

            # Deserialize with fastavro
            buffer = BytesIO(avro_payload)
            obj_dict = fastavro.schemaless_reader(buffer, parsed_schema)

            # Convert dictionary to AvroModel object
            obj = target_class.parse_obj(obj_dict)

            return obj, registry_schema

        except Exception as e:
            if isinstance(e, DeserializationError):
                raise
            raise DeserializationError(
                f"Failed to deserialize message with schema: {e}"
            )

    async def register_schema(
        self, obj: AvroModel, group_id: str, artifact_id: str
    ) -> int:
        """Register a schema derived from an AvroModel object.

        Note: This is currently not implemented as Apicurio v3 API registration
        requires additional authentication and may vary by deployment.

        Args:
            obj: AvroModel object to derive schema from
            group_id: Group ID to register schema under
            artifact_id: Artifact ID to register schema under

        Returns:
            Global ID of the registered schema

        Raises:
            NotImplementedError: Always raised as this feature is not implemented
        """
        raise NotImplementedError(
            "Schema registration is not implemented. "
            "Please register schemas manually through the Apicurio Registry UI or API."
        )

    async def _get_parsed_schema(
        self, schema_id: int, registry_schema: dict[str, Any]
    ) -> dict[str, Any]:
        """Get parsed schema for fastavro, with caching if enabled.

        Args:
            schema_id: Schema ID for caching
            registry_schema: Raw schema from registry

        Returns:
            Parsed schema ready for fastavro
        """
        if self.cache_schemas and schema_id in self._parsed_schema_cache:
            return self._parsed_schema_cache[schema_id]

        # Extract schema content
        if isinstance(registry_schema, dict) and "schema" in registry_schema:
            schema_content = registry_schema["schema"]
        else:
            schema_content = registry_schema

        # Parse schema content if it's a string
        if isinstance(schema_content, str):
            try:
                parsed_schema = json.loads(schema_content)
            except json.JSONDecodeError:
                raise DeserializationError(f"Invalid JSON schema for ID {schema_id}")
        else:
            parsed_schema = schema_content

        # Cache if enabled
        if self.cache_schemas:
            self._parsed_schema_cache[schema_id] = parsed_schema

        return parsed_schema

    def _validate_data_against_schema(
        self, data: dict[str, Any], schema: dict[str, Any]
    ) -> None:
        """Validate data against schema.

        Args:
            data: Data dictionary to validate
            schema: Avro schema to validate against

        Raises:
            SchemaMismatchError: If data doesn't match schema
        """
        try:
            # Use fastavro's validation
            buffer = BytesIO()
            fastavro.schemaless_writer(buffer, schema, data)
        except Exception as e:
            raise SchemaMismatchError(f"Data does not match schema: {e}")
