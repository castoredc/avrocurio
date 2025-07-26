"""End-to-end integration tests with real Apicurio Registry."""

import time
from dataclasses import dataclass

import pytest
from dataclasses_avroschema import AvroModel

from avrocurio import ApicurioClient, AvroSerializer
from avrocurio.exceptions import SchemaMatchError, SchemaNotFoundError
from avrocurio.wire_format import ConfluentWireFormat


@dataclass
class IntegrationTestEvent(AvroModel):
    """Simple test schema for integration tests."""

    id: str
    timestamp: int
    message: str


@pytest.mark.integration
class TestEndToEndFlow:
    """Integration tests for complete AvroCurio workflow with real Apicurio."""

    async def test_basic_round_trip_with_registered_schema(
        self,
        serializer: AvroSerializer,
        apicurio_client: ApicurioClient,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test complete serialize → deserialize round trip with schema registration."""
        # Create test event
        test_event = IntegrationTestEvent(
            id=test_artifact_id,
            timestamp=int(time.time()),
            message="Hello from AvroCurio integration test!",
        )

        # Register our test schema
        group_id = test_group_id
        artifact_id = test_artifact_id
        schema_content = test_event.avro_schema()

        # Register the schema (will handle existing schemas gracefully)
        global_id = await apicurio_client.register_schema(
            group=group_id, artifact_name=artifact_id, schema_content=schema_content
        )

        # Test serialization with the registered schema
        serialized = await serializer.serialize(test_event, global_id)

        # Verify wire format structure
        assert len(serialized) >= 5  # magic byte + 4 bytes schema ID + payload
        assert serialized[0] == 0x0  # Magic byte

        # Test deserialization
        deserialized = await serializer.deserialize(serialized, IntegrationTestEvent)

        # Verify round trip integrity
        assert deserialized.id == test_event.id
        assert deserialized.timestamp == test_event.timestamp
        assert deserialized.message == test_event.message

    async def test_schema_retrieval_by_subject(
        self, apicurio_client: ApicurioClient, test_group_id: str, test_artifact_id: str
    ):
        """Test schema retrieval using group/artifact ID (subject)."""
        # Create and register a test schema
        test_event = IntegrationTestEvent(
            id=test_artifact_id,
            timestamp=int(time.time()),
            message="Schema retrieval test!",
        )

        group_id = test_group_id
        artifact_id = f"{test_artifact_id}-retrieval"
        schema_content = test_event.avro_schema()

        # Register the schema
        registered_global_id = await apicurio_client.register_schema(
            group=group_id, artifact_name=artifact_id, schema_content=schema_content
        )

        # Test retrieving latest schema for the artifact we just registered
        global_id, schema = await apicurio_client.get_latest_schema(group_id, artifact_id)

        # Verify we got valid results
        assert isinstance(global_id, int)
        assert global_id > 0
        assert isinstance(schema, dict)
        assert global_id == registered_global_id

        # Verify we can also get schema by global ID
        retrieved_schema = await apicurio_client.get_schema_by_global_id(global_id)
        assert retrieved_schema is not None

    async def test_wire_format_structure(
        self, apicurio_client: ApicurioClient, test_group_id: str, test_artifact_id: str
    ):
        """Test Confluent wire format compliance with real schema."""
        # Create and register a test schema
        test_event = IntegrationTestEvent(id=test_artifact_id, timestamp=int(time.time()), message="Wire format test!")

        group_id = test_group_id
        artifact_id = f"{test_artifact_id}-wire-format"
        schema_content = test_event.avro_schema()

        # Register the schema
        global_id = await apicurio_client.register_schema(
            group=group_id, artifact_name=artifact_id, schema_content=schema_content
        )

        # Create dummy payload (we're just testing wire format structure)
        dummy_payload = b"dummy avro data"

        # Test wire format encoding
        wire_message = ConfluentWireFormat.encode(global_id, dummy_payload)

        # Verify wire format structure
        assert len(wire_message) >= 5  # magic byte + 4 bytes schema ID + payload
        assert wire_message[0] == 0x0  # Magic byte

        # Test wire format decoding
        decoded_schema_id, decoded_payload = ConfluentWireFormat.decode(wire_message)

        assert decoded_schema_id == global_id
        assert decoded_payload == dummy_payload

    async def test_schema_not_found_error(self, apicurio_client: ApicurioClient):
        """Test error handling for missing schemas."""
        # Try to get a schema with an ID that definitely doesn't exist
        non_existent_id = 999999999

        with pytest.raises(
            SchemaNotFoundError,
            match=f"Schema with global ID {non_existent_id} not found",
        ):
            await apicurio_client.get_schema_by_global_id(non_existent_id)

    async def test_registry_connectivity(
        self, apicurio_client: ApicurioClient, test_group_id: str, test_artifact_id: str
    ):
        """Test basic connectivity to Apicurio Registry."""
        # This test verifies we can connect and perform basic operations
        artifacts = await apicurio_client.search_artifacts()

        # Should return a list (even if empty)
        assert isinstance(artifacts, list)

        # Test that we can register a schema to verify write access
        test_event = IntegrationTestEvent(
            id=test_artifact_id,
            timestamp=int(time.time()),
            message="Connectivity test!",
        )

        group_id = test_group_id
        artifact_id = f"{test_artifact_id}-connectivity"
        schema_content = test_event.avro_schema()

        # Register a schema to test write operations
        global_id = await apicurio_client.register_schema(
            group=group_id, artifact_name=artifact_id, schema_content=schema_content
        )

        assert global_id is not None
        assert isinstance(global_id, int)

        # Verify we can retrieve the schema we just registered
        retrieved_global_id, schema = await apicurio_client.get_latest_schema(group_id, artifact_id)
        assert retrieved_global_id == global_id
        assert schema is not None

    async def test_serialize_automatic_lookup_registry_wide(
        self,
        serializer: AvroSerializer,
        apicurio_client: ApicurioClient,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test serialize method with automatic lookup across entire registry."""
        # Create and register a test schema
        test_event = IntegrationTestEvent(
            id=test_artifact_id,
            timestamp=int(time.time()),
            message="Serialize automatic lookup test!",
        )

        # Register the schema
        await apicurio_client.register_schema(
            group=test_group_id,
            artifact_name=f"{test_artifact_id}-serialize-auto",
            schema_content=test_event.avro_schema(),
        )

        # Test serialize with automatic lookup (no schema_id provided)
        serialized = await serializer.serialize(test_event)

        # Test deserialization to verify round trip
        deserialized = await serializer.deserialize(serialized, IntegrationTestEvent)

        assert deserialized.id == test_event.id
        assert deserialized.timestamp == test_event.timestamp
        assert deserialized.message == test_event.message

    async def test_serialize_explicit_schema(
        self,
        serializer: AvroSerializer,
        apicurio_client: ApicurioClient,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test that serialize with explicit schema_id works."""
        # Create and register a test schema
        test_event = IntegrationTestEvent(
            id=test_artifact_id,
            timestamp=int(time.time()),
            message="Serialize with explicit schema ID",
        )

        # Register the schema
        global_id = await apicurio_client.register_schema(
            group=test_group_id,
            artifact_name=f"{test_artifact_id}-serialize-schema-id",
            schema_content=test_event.avro_schema(),
        )

        serialized = await serializer.serialize(test_event, schema_id=global_id)
        # Test deserialization to verify round trip
        deserialized = await serializer.deserialize(serialized, IntegrationTestEvent)

        assert deserialized.id == test_event.id
        assert deserialized.timestamp == test_event.timestamp
        assert deserialized.message == test_event.message

    async def test_serialize_automatic_lookup_no_match_integration(
        self,
        serializer: AvroSerializer,
    ):
        """Test serialize automatic lookup when no matching schema exists."""

        # Create an event with a unique schema that won't exist in registry
        @dataclass
        class VeryUniqueTestEvent(AvroModel):
            extremely_unique_field: str
            another_super_specific_field: int
            totally_never_registered_field: bool

        unique_event = VeryUniqueTestEvent(
            extremely_unique_field="serialize-unique",
            another_super_specific_field=999,
            totally_never_registered_field=False,
        )

        # Test that SchemaMatchError is raised for serialize() automatic lookup
        with pytest.raises(SchemaMatchError, match="No matching schema found in registry for automatic lookup"):
            await serializer.serialize(unique_event)

    async def test_register_schema_end_to_end_workflow(
        self,
        serializer: AvroSerializer,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test complete end-to-end workflow using serializer.register_schema."""
        # Create test event
        test_event = IntegrationTestEvent(
            id=f"{test_artifact_id}-e2e",
            timestamp=int(time.time()),
            message="End-to-end register_schema workflow test!",
        )

        # Register schema using serializer (not directly via client)
        global_id = await serializer.register_schema(
            obj=test_event,
            group=test_group_id,
            artifact_name=f"{test_artifact_id}-e2e-workflow",
        )

        # Verify registration succeeded
        assert isinstance(global_id, int)
        assert global_id > 0

        # Test serialization with returned global ID
        serialized = await serializer.serialize(test_event, schema_id=global_id)
        assert len(serialized) >= 5
        assert serialized[0] == 0x0

        # Test deserialization
        deserialized = await serializer.deserialize(serialized, IntegrationTestEvent)
        assert deserialized.id == test_event.id
        assert deserialized.timestamp == test_event.timestamp
        assert deserialized.message == test_event.message

        # Test that the registered schema can now be found via automatic lookup
        serialized_auto = await serializer.serialize(test_event)
        deserialized_auto = await serializer.deserialize(serialized_auto, IntegrationTestEvent)

        assert deserialized_auto.id == test_event.id
        assert deserialized_auto.timestamp == test_event.timestamp
        assert deserialized_auto.message == test_event.message

    async def test_deserialize_to_raw_dict(
        self,
        serializer: AvroSerializer,
        apicurio_client: ApicurioClient,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test deserialization to raw dictionary with real Apicurio Registry."""
        test_event = IntegrationTestEvent(
            id=f"{test_artifact_id}-raw-dict",
            timestamp=int(time.time()),
            message="Raw dictionary deserialization integration test!",
        )

        global_id = await apicurio_client.register_schema(
            group=test_group_id,
            artifact_name=f"{test_artifact_id}-raw-dict",
            schema_content=test_event.avro_schema(),
        )

        serialized = await serializer.serialize(test_event, global_id)
        deserialized = await serializer.deserialize(serialized)

        # Verify result is a dictionary with expected data
        assert isinstance(deserialized, dict)
        assert deserialized["id"] == test_event.id
        assert deserialized["timestamp"] == test_event.timestamp
        assert deserialized["message"] == test_event.message

        # Also test with explicit None parameter for completeness
        assert await serializer.deserialize(serialized, None) == deserialized
