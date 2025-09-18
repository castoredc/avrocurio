"""Tests for Avro serializer/deserializer."""

import struct
from unittest.mock import AsyncMock

import pytest

from avrocurio.config import ApicurioConfig
from avrocurio.exceptions import DeserializationError, SchemaMatchError, SchemaRegistrationError, SerializationError
from avrocurio.schema_client import ApicurioClient
from avrocurio.serializer import AvroSerializer

from .test_schemas import ComplexUser, SimpleUser


@pytest.fixture
def config():
    """Test configuration."""
    return ApicurioConfig(base_url="http://test-registry:8080")


@pytest.fixture
def mock_client():
    """Mock ApicurioClient."""
    return AsyncMock(spec=ApicurioClient)


@pytest.fixture
def serializer(mock_client):
    """Test serializer with mocked client."""
    return AvroSerializer(mock_client)


@pytest.fixture
def simple_user_schema():
    """Simple user Avro schema."""
    return {
        "type": "record",
        "name": "SimpleUser",
        "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}],
    }


@pytest.fixture
def complex_user_schema():
    """Complex user Avro schema."""
    return {
        "type": "record",
        "name": "ComplexUser",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "email", "type": ["null", "string"], "default": None},
            {"name": "is_active", "type": "boolean", "default": True},
        ],
    }


class TestAvroSerializer:
    """Test cases for AvroSerializer."""

    def test_init(self, mock_client):
        """Test serializer initialization."""
        serializer = AvroSerializer(mock_client)
        assert serializer.client == mock_client

    @pytest.mark.asyncio
    async def test_serialize_success(self, serializer, simple_user_schema):
        """Test successful serialization."""
        user = SimpleUser(name="John Doe", age=30)
        schema_id = 12345

        # Mock client response
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema

        # Test serialization
        result = await serializer.serialize(user, schema_id)

        # Verify result structure
        assert isinstance(result, bytes)
        assert len(result) > 5  # Should have header + payload
        assert result[0] == 0x0  # Magic byte

        # Verify client was called
        serializer.client.get_schema_by_global_id.assert_called_once_with(schema_id)

    @pytest.mark.asyncio
    async def test_serialize_schema_mismatch(self, serializer):
        """Test serialization with schema mismatch."""
        # Create user with data
        user = SimpleUser(name="Test", age=30)
        schema_id = 12345

        # Mock client response with schema requiring different field names
        incompatible_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "full_name", "type": "string"},  # Different field name
                {"name": "years", "type": "int"},  # Different field name
            ],
        }
        serializer.client.get_schema_by_global_id.return_value = incompatible_schema

        with pytest.raises(SerializationError, match="Data does not match schema"):
            await serializer.serialize(user, schema_id)

    @pytest.mark.parametrize(
        ("user_class", "user_data", "schema_fixture"),
        [
            (SimpleUser, {"name": "Alice Johnson", "age": 28}, "simple_user_schema"),
            (
                ComplexUser,
                {"name": "Complex User", "age": 30, "email": "user@example.com", "is_active": True},
                "complex_user_schema",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_serialization_round_trip(self, serializer, user_class, user_data, schema_fixture, request):
        """Test successful serialization and deserialization round-trip."""
        schema = request.getfixturevalue(schema_fixture)
        original_user = user_class(**user_data)
        schema_id = 12345

        # Mock client response
        serializer.client.get_schema_by_global_id.return_value = schema

        # Serialize
        message = await serializer.serialize(original_user, schema_id)
        assert isinstance(message, bytes)
        assert message[0] == 0x0  # Magic byte

        # Reset mock for deserialization
        serializer.client.reset_mock()
        serializer.client.get_schema_by_global_id.return_value = schema

        # Deserialize
        result = await serializer.deserialize(message, user_class)
        assert isinstance(result, user_class)

        # Compare all fields
        for field, expected_value in user_data.items():
            assert getattr(result, field) == expected_value

        # Verify client was called
        serializer.client.get_schema_by_global_id.assert_called_once_with(schema_id)

    @pytest.mark.asyncio
    async def test_deserialize_invalid_wire_format(self, serializer):
        """Test deserialization with invalid wire format."""
        # Invalid message (wrong magic byte)
        invalid_message = b"\x01\x00\x00\x00\x01test"

        with pytest.raises(DeserializationError, match="Failed to deserialize message"):
            await serializer.deserialize(invalid_message, SimpleUser)

    @pytest.mark.asyncio
    async def test_register_schema_success(self, serializer):
        """Test successful schema registration."""
        user = SimpleUser(name="Test", age=30)
        expected_global_id = 12345

        # Mock client response
        serializer.client.register_schema.return_value = expected_global_id

        # Test schema registration
        result = await serializer.register_schema(user, "test-group", "user-schema")
        assert result == expected_global_id

        # Verify client was called with correct parameters
        serializer.client.register_schema.assert_called_once_with(
            group="test-group", artifact_name="user-schema", schema_content=user.avro_schema()
        )

    @pytest.mark.asyncio
    async def test_register_schema_client_error(self, serializer):
        """Test schema registration error handling."""
        user = SimpleUser(name="Test", age=30)

        # Mock client to raise an error
        serializer.client.register_schema.side_effect = Exception("Registry connection failed")

        # Test that error is wrapped in SerializationError
        with pytest.raises(SchemaRegistrationError, match="Failed to register schema for test-group/user-schema"):
            await serializer.register_schema(user, "test-group", "user-schema")

    @pytest.mark.parametrize(
        ("error_scenario", "setup_mocks", "expected_exception", "expected_message"),
        [
            (
                "serialization_network_error",
                lambda serializer: setattr(
                    serializer.client, "get_schema_by_global_id", AsyncMock(side_effect=Exception("Network error"))
                ),
                SerializationError,
                "Failed to serialize object",
            ),
            (
                "deserialization_invalid_content",
                lambda serializer: setattr(
                    serializer.client, "get_schema_by_global_id", AsyncMock(return_value={"type": "string"})
                ),
                DeserializationError,
                "Failed to deserialize message",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_error_handling(self, serializer, error_scenario, setup_mocks, expected_exception, expected_message):
        """Test various error handling scenarios."""
        user = SimpleUser(name="Test", age=30)

        if error_scenario == "serialization_network_error":
            setup_mocks(serializer)
            with pytest.raises(expected_exception, match=expected_message):
                await serializer.serialize(user, 12345)
        elif error_scenario == "deserialization_invalid_content":
            setup_mocks(serializer)
            invalid_message = b"\x00\x00\x00\x30\x39invalid avro data"
            with pytest.raises(expected_exception, match=expected_message):
                await serializer.deserialize(invalid_message, SimpleUser)

    @pytest.mark.asyncio
    async def test_serialize_auto_lookup_content_success(self, serializer, simple_user_schema):
        """Test serialize with automatic lookup using content search."""
        user = SimpleUser(name="Auto Lookup User", age=50)
        schema_id = 88888

        # Setup mocks for content success
        serializer.client.find_artifact_by_content.return_value = ("found-group", "found-artifact")
        serializer.client.get_latest_schema.return_value = (schema_id, simple_user_schema)
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema

        result = await serializer.serialize(user)
        assert isinstance(result, bytes)
        assert result[0] == 0x0  # Magic byte

        # Verify expected method calls
        serializer.client.find_artifact_by_content.assert_called_once()
        serializer.client.get_latest_schema.assert_called_once()
        serializer.client.get_schema_by_global_id.assert_called_once()

    @pytest.mark.asyncio
    async def test_serialize_auto_lookup_no_match(self, serializer):
        """Test serialize automatic lookup when no matching schema found."""
        user = SimpleUser(name="Auto Lookup User", age=50)

        # Setup mocks for no match
        serializer.client.find_artifact_by_content.return_value = None

        with pytest.raises(SchemaMatchError, match="No matching schema found in registry for automatic lookup"):
            await serializer.serialize(user)

        # Verify expected method calls
        serializer.client.find_artifact_by_content.assert_called_once()

    @pytest.mark.asyncio
    async def test_serialize_backward_compatibility(self, serializer, simple_user_schema):
        """Test that explicit schema_id usage still works (backward compatibility)."""
        user = SimpleUser(name="Backward Compat User", age=45)
        schema_id = 66666

        # Mock traditional behavior
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema

        # Test explicit schema_id (original API)
        result = await serializer.serialize(user, schema_id=schema_id)

        assert isinstance(result, bytes)
        assert result[0] == 0x0  # Magic byte

        # Verify only get_schema_by_global_id was called, not search methods
        serializer.client.get_schema_by_global_id.assert_called_once_with(schema_id)

        # Verify search methods were not called
        assert (
            not hasattr(serializer.client, "find_artifact_by_content")
            or not serializer.client.find_artifact_by_content.called
        )

    @pytest.mark.asyncio
    async def test_deserialize_to_dict(self, serializer, simple_user_schema):
        """Test deserialization to raw dictionary when target_class is None."""
        user = SimpleUser(name="Dict User", age=35)
        schema_id = 99999

        # Mock client response for serialization
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema

        # Serialize first
        message = await serializer.serialize(user, schema_id)

        # Reset mock for deserialization
        serializer.client.reset_mock()
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema

        # Deserialize to dict
        result = await serializer.deserialize(message)

        # Verify result is a dictionary with expected data
        assert isinstance(result, dict)
        assert result == {"name": "Dict User", "age": 35}

        # Verify client was called
        serializer.client.get_schema_by_global_id.assert_called_once_with(schema_id)

    @pytest.mark.asyncio
    async def test_deserialize_to_dict_complex_schema(self, serializer, complex_user_schema):
        """Test deserialization to raw dictionary with complex schema."""
        user = ComplexUser(name="Complex Dict User", age=40, email="test@example.com", is_active=False)
        schema_id = 88888

        # Mock client response for serialization
        serializer.client.get_schema_by_global_id.return_value = complex_user_schema

        # Serialize first
        message = await serializer.serialize(user, schema_id)

        # Reset mock for deserialization
        serializer.client.reset_mock()
        serializer.client.get_schema_by_global_id.return_value = complex_user_schema

        # Deserialize to dict
        result = await serializer.deserialize(message, None)

        # Verify result is a dictionary with expected data
        assert isinstance(result, dict)
        expected_dict = {
            "name": "Complex Dict User",
            "age": 40,
            "email": "test@example.com",
            "is_active": False,
        }
        assert result == expected_dict

    @pytest.mark.asyncio
    async def test_deserialize_to_dict_error_handling(self, serializer):
        """Test error handling when deserializing to dict with invalid data."""
        # Invalid message (wrong magic byte)
        invalid_message = b"\x01\x00\x00\x00\x01test"

        with pytest.raises(DeserializationError, match="Failed to deserialize message"):
            await serializer.deserialize(invalid_message, None)

    @pytest.mark.asyncio
    async def test_deserialize_backward_compatibility(self, serializer, simple_user_schema):
        """Test that existing AvroModel deserialization still works unchanged."""
        user = SimpleUser(name="Compatibility User", age=25)
        schema_id = 77777

        # Mock client response for serialization
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema

        # Serialize first
        message = await serializer.serialize(user, schema_id)

        # Reset mock for deserialization
        serializer.client.reset_mock()
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema

        # Deserialize to AvroModel (existing behavior)
        result = await serializer.deserialize(message, SimpleUser)

        # Verify result is AvroModel instance with expected data
        assert isinstance(result, SimpleUser)
        assert result.name == "Compatibility User"
        assert result.age == 25

    @pytest.mark.asyncio
    async def test_deserialize_non_bytes_type(self, serializer):
        # Create a string that looks like valid Confluent wire format
        magic_byte = b"\x00"
        schema_id = struct.pack(">I", 12345)
        payload = b"test_payload"
        message = magic_byte + schema_id + payload

        # Convert to string (this is what we want to prevent)
        str_message = message.decode("latin1")
        with pytest.raises(DeserializationError, match="message is of type str, but should be bytes"):
            await serializer.deserialize(str_message, SimpleUser)
