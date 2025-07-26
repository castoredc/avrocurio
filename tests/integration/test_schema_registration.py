"""Integration tests for AvroSerializer schema registration functionality."""

import time
from dataclasses import dataclass

import pytest
from dataclasses_avroschema import AvroModel

from avrocurio import AvroSerializer


@dataclass
class RegistrationTestEvent(AvroModel):
    """Test schema for registration integration tests."""

    event_id: str
    timestamp: int
    data: str
    version: int = 1

    class Meta:
        schema_name = "RegistrationTestEvent"


@dataclass
class UpdatedRegistrationTestEvent(AvroModel):
    """Updated test schema for testing schema evolution."""

    event_id: str
    timestamp: int
    data: str
    version: int = 2
    new_field: str | None = None

    class Meta:
        schema_name = "RegistrationTestEvent"


@pytest.mark.integration
class TestSerializerRegistration:
    """Integration tests for AvroSerializer register_schema method."""

    async def test_register_schema_success(
        self,
        serializer: AvroSerializer,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test successful schema registration."""
        # Create test event
        test_event = RegistrationTestEvent(
            event_id=test_artifact_id,
            timestamp=int(time.time()),
            data="Registration test data",
        )

        # Register the schema
        global_id = await serializer.register_schema(
            obj=test_event,
            group=test_group_id,
            artifact_name=test_artifact_id,
        )

        # Verify we got a valid global ID
        assert isinstance(global_id, int)
        assert global_id > 0

        # Verify we can retrieve the schema using the returned global ID
        schema = await serializer.client.get_schema_by_global_id(global_id)
        assert schema is not None
        assert isinstance(schema, dict)

    async def test_register_schema_idempotent(
        self,
        serializer: AvroSerializer,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test that registering identical schema content returns the same global ID."""
        test_event = RegistrationTestEvent(
            event_id=test_artifact_id,
            timestamp=int(time.time()),
            data="Duplicate registration test",
        )

        # Register the schema for the first time
        first_global_id = await serializer.register_schema(
            obj=test_event,
            group=test_group_id,
            artifact_name=f"{test_artifact_id}-duplicate",
        )

        # Register the same schema again
        second_global_id = await serializer.register_schema(
            obj=test_event,
            group=test_group_id,
            artifact_name=f"{test_artifact_id}-duplicate",
        )

        # Both registrations should succeed and return the same global ID
        # since the schema content is identical (idempotent behavior)
        assert isinstance(first_global_id, int)
        assert isinstance(second_global_id, int)
        assert first_global_id > 0
        assert second_global_id > 0
        assert first_global_id == second_global_id

    async def test_register_schema_version_update(
        self,
        serializer: AvroSerializer,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test updating an existing artifact with an evolved schema version."""
        # Register initial schema version
        initial_event = RegistrationTestEvent(
            event_id=test_artifact_id,
            timestamp=int(time.time()),
            data="Evolution test - v1",
        )

        first_global_id = await serializer.register_schema(
            obj=initial_event,
            group=test_group_id,
            artifact_name=f"{test_artifact_id}-evolution",
        )

        # Register evolved schema version
        evolved_event = UpdatedRegistrationTestEvent(
            event_id=test_artifact_id,
            timestamp=int(time.time()),
            data="Evolution test - v2",
            new_field="New field value",
        )

        second_global_id = await serializer.register_schema(
            obj=evolved_event,
            group=test_group_id,
            artifact_name=f"{test_artifact_id}-evolution",
        )

        # Both should succeed and have different global IDs
        assert isinstance(first_global_id, int)
        assert isinstance(second_global_id, int)
        assert first_global_id != second_global_id

        # Verify both schemas can be retrieved
        first_schema = await serializer.client.get_schema_by_global_id(first_global_id)
        second_schema = await serializer.client.get_schema_by_global_id(second_global_id)

        assert first_schema != second_schema

        # Test serialization/deserialization with both versions
        # Serialize v1 data with v1 schema
        v1_serialized = await serializer.serialize(initial_event, schema_id=first_global_id)
        v1_deserialized = await serializer.deserialize(v1_serialized, RegistrationTestEvent)
        assert v1_deserialized.event_id == initial_event.event_id
        assert v1_deserialized.version == 1

        # Serialize v2 data with v2 schema
        v2_serialized = await serializer.serialize(evolved_event, schema_id=second_global_id)
        v2_deserialized = await serializer.deserialize(v2_serialized, UpdatedRegistrationTestEvent)
        assert v2_deserialized.event_id == evolved_event.event_id
        assert v2_deserialized.version == 2
        assert v2_deserialized.new_field == "New field value"

        # Test backwards compatibility: v1 data with v2 schema
        # Data serialized with v1 should be readable with v2 (new field has default)
        v1_data_v2_schema = await serializer.deserialize(v1_serialized, UpdatedRegistrationTestEvent)
        assert v1_data_v2_schema.event_id == initial_event.event_id
        assert v1_data_v2_schema.version == 1  # Original version value
        assert v1_data_v2_schema.new_field is None  # Default value for missing field

    async def test_register_then_serialize_with_returned_id(
        self,
        serializer: AvroSerializer,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Register schema then use returned ID for serialization."""
        test_event = RegistrationTestEvent(
            event_id=test_artifact_id,
            timestamp=int(time.time()),
            data="Register then serialize test",
        )

        # Register the schema
        global_id = await serializer.register_schema(
            obj=test_event,
            group=test_group_id,
            artifact_name=f"{test_artifact_id}-serialize",
        )

        # Use the returned global ID for serialization
        serialized = await serializer.serialize(test_event, schema_id=global_id)

        # Verify serialization worked
        assert isinstance(serialized, bytes)
        assert len(serialized) >= 5  # magic byte + 4 bytes schema ID + payload
        assert serialized[0] == 0x0  # Magic byte

        # Test deserialization round trip
        deserialized = await serializer.deserialize(serialized, RegistrationTestEvent)

        # Verify all fields match
        assert deserialized.event_id == test_event.event_id
        assert deserialized.timestamp == test_event.timestamp
        assert deserialized.data == test_event.data
        assert deserialized.version == test_event.version

    async def test_register_then_automatic_lookup(
        self,
        serializer: AvroSerializer,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test that registered schemas can be found via automatic lookup."""
        test_event = RegistrationTestEvent(
            event_id=test_artifact_id,
            timestamp=int(time.time()),
            data="Register then auto lookup test",
        )

        # Register the schema
        await serializer.register_schema(
            obj=test_event,
            group=test_group_id,
            artifact_name=f"{test_artifact_id}-autolookup",
        )

        # Use automatic lookup for serialization (no schema_id provided)
        serialized = await serializer.serialize(test_event)

        # Verify serialization worked
        assert isinstance(serialized, bytes)
        assert len(serialized) >= 5
        assert serialized[0] == 0x0

        # Test deserialization round trip
        deserialized = await serializer.deserialize(serialized, RegistrationTestEvent)
        assert deserialized.event_id == test_event.event_id
        assert deserialized.timestamp == test_event.timestamp
        assert deserialized.data == test_event.data
