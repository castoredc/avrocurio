"""Tests for Avro serializer/deserializer."""

import json
import pytest
from io import BytesIO
from unittest.mock import AsyncMock, patch

import fastavro

from avrocurio.serializer import AvroSerializer
from avrocurio.schema_client import ApicurioClient
from avrocurio.config import ApicurioConfig
from avrocurio.exceptions import SerializationError, DeserializationError, SchemaMismatchError
from .test_schemas import SimpleUser, ComplexUser, Product


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
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
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
            {"name": "is_active", "type": "boolean", "default": True}
        ]
    }


class TestAvroSerializer:
    """Test cases for AvroSerializer."""
    
    def test_init_default(self, mock_client):
        """Test serializer initialization with defaults."""
        serializer = AvroSerializer(mock_client)
        
        assert serializer.client == mock_client
        assert serializer.cache_schemas is True
        assert serializer._parsed_schema_cache == {}
    
    def test_init_no_cache(self, mock_client):
        """Test serializer initialization without caching."""
        serializer = AvroSerializer(mock_client, cache_schemas=False)
        
        assert serializer.client == mock_client
        assert serializer.cache_schemas is False
        assert serializer._parsed_schema_cache == {}
    
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
    async def test_serialize_with_schema_wrapper(self, serializer, simple_user_schema):
        """Test serialization with schema wrapped in 'schema' field."""
        user = SimpleUser(name="Jane Doe", age=25)
        schema_id = 12345
        
        # Mock client response with schema wrapper
        wrapped_schema = {"schema": json.dumps(simple_user_schema)}
        serializer.client.get_schema_by_global_id.return_value = wrapped_schema
        
        # Test serialization
        result = await serializer.serialize(user, schema_id)
        
        assert isinstance(result, bytes)
        assert result[0] == 0x0  # Magic byte
    
    
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
                {"name": "years", "type": "int"}  # Different field name
            ]
        }
        serializer.client.get_schema_by_global_id.return_value = incompatible_schema
        
        # Should raise SchemaMismatchError
        with pytest.raises(SchemaMismatchError):
            await serializer.serialize(user, schema_id)
    
    @pytest.mark.asyncio
    async def test_deserialize_success(self, serializer, simple_user_schema):
        """Test successful deserialization."""
        # Create test data
        original_user = SimpleUser(name="Alice Johnson", age=28)
        schema_id = 12345
        
        # Mock serialization to get test message
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema
        message = await serializer.serialize(original_user, schema_id)
        
        # Reset mock for deserialization
        serializer.client.reset_mock()
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema
        
        # Test deserialization
        result = await serializer.deserialize(message, SimpleUser)
        
        assert isinstance(result, SimpleUser)
        assert result.name == original_user.name
        assert result.age == original_user.age
        
        # Verify client was called
        serializer.client.get_schema_by_global_id.assert_called_once_with(schema_id)
    
    @pytest.mark.asyncio
    async def test_deserialize_with_schema_success(self, serializer, simple_user_schema):
        """Test successful deserialization with schema return."""
        # Create test data
        original_user = SimpleUser(name="Charlie Brown", age=40)
        schema_id = 54321
        
        # Mock serialization to get test message
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema
        message = await serializer.serialize(original_user, schema_id)
        
        # Reset mock for deserialization
        serializer.client.reset_mock()
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema
        
        # Test deserialization with schema
        result_user, result_schema = await serializer.deserialize_with_schema(message, SimpleUser)
        
        assert isinstance(result_user, SimpleUser)
        assert result_user.name == original_user.name
        assert result_user.age == original_user.age
        assert result_schema == simple_user_schema
    
    @pytest.mark.asyncio
    async def test_deserialize_invalid_wire_format(self, serializer):
        """Test deserialization with invalid wire format."""
        # Invalid message (wrong magic byte)
        invalid_message = b"\x01\x00\x00\x00\x01test"
        
        with pytest.raises(DeserializationError, match="Failed to deserialize message"):
            await serializer.deserialize(invalid_message, SimpleUser)
    
    @pytest.mark.asyncio
    async def test_register_schema_not_implemented(self, serializer):
        """Test that schema registration raises NotImplementedError."""
        user = SimpleUser(name="Test", age=30)
        
        with pytest.raises(NotImplementedError, match="Schema registration is not implemented"):
            await serializer.register_schema(user, "default", "user-schema")
    
    @pytest.mark.asyncio
    async def test_schema_caching_enabled(self, mock_client, simple_user_schema):
        """Test schema caching when enabled."""
        serializer = AvroSerializer(mock_client, cache_schemas=True)
        schema_id = 12345
        
        mock_client.get_schema_by_global_id.return_value = simple_user_schema
        
        # First call should fetch and cache
        result1 = await serializer._get_parsed_schema(schema_id, simple_user_schema)
        assert result1 == simple_user_schema
        assert schema_id in serializer._parsed_schema_cache
        
        # Second call should use cache
        result2 = await serializer._get_parsed_schema(schema_id, simple_user_schema)
        assert result2 == simple_user_schema
        assert result1 is result2  # Should be same object from cache
    
    @pytest.mark.asyncio
    async def test_schema_caching_disabled(self, mock_client, simple_user_schema):
        """Test schema caching when disabled."""
        serializer = AvroSerializer(mock_client, cache_schemas=False)
        schema_id = 12345
        
        # Multiple calls should not cache
        result1 = await serializer._get_parsed_schema(schema_id, simple_user_schema)
        result2 = await serializer._get_parsed_schema(schema_id, simple_user_schema)
        
        assert result1 == simple_user_schema
        assert result2 == simple_user_schema
        assert schema_id not in serializer._parsed_schema_cache
    
    @pytest.mark.asyncio
    async def test_get_parsed_schema_string_content(self, serializer, simple_user_schema):
        """Test parsing schema from string content."""
        schema_id = 12345
        registry_schema = {"schema": json.dumps(simple_user_schema)}
        
        result = await serializer._get_parsed_schema(schema_id, registry_schema)
        
        assert result == simple_user_schema
    
    @pytest.mark.asyncio
    async def test_get_parsed_schema_invalid_json(self, serializer):
        """Test parsing invalid JSON schema."""
        schema_id = 12345
        registry_schema = {"schema": "invalid json{"}
        
        with pytest.raises(DeserializationError, match="Invalid JSON schema"):
            await serializer._get_parsed_schema(schema_id, registry_schema)
    
    @pytest.mark.asyncio
    async def test_validate_data_against_schema_success(self, serializer, simple_user_schema):
        """Test successful data validation."""
        data = {"name": "Test User", "age": 25}
        
        # Should not raise exception
        serializer._validate_data_against_schema(data, simple_user_schema)
    
    @pytest.mark.asyncio
    async def test_validate_data_against_schema_failure(self, serializer, simple_user_schema):
        """Test data validation failure."""
        # Missing required field
        invalid_data = {"name": "Test User"}  # Missing age
        
        with pytest.raises(SchemaMismatchError, match="Data does not match schema"):
            serializer._validate_data_against_schema(invalid_data, simple_user_schema)
    
    @pytest.mark.asyncio
    async def test_complex_schema_serialization(self, serializer, complex_user_schema):
        """Test serialization with complex schema including optional fields."""
        user = ComplexUser(name="Complex User", age=30, email="user@example.com", is_active=True)
        schema_id = 99999
        
        serializer.client.get_schema_by_global_id.return_value = complex_user_schema
        
        # Test serialization
        result = await serializer.serialize(user, schema_id)
        
        assert isinstance(result, bytes)
        assert result[0] == 0x0  # Magic byte
        
        # Test round-trip
        serializer.client.reset_mock()
        serializer.client.get_schema_by_global_id.return_value = complex_user_schema
        
        deserialized = await serializer.deserialize(result, ComplexUser)
        assert deserialized.name == user.name
        assert deserialized.age == user.age
        assert deserialized.email == user.email
        assert deserialized.is_active == user.is_active
    
    @pytest.mark.asyncio
    async def test_serialization_error_handling(self, serializer):
        """Test serialization error handling."""
        user = SimpleUser(name="Test", age=30)
        schema_id = 12345
        
        # Mock client to raise exception
        serializer.client.get_schema_by_global_id.side_effect = Exception("Network error")
        
        with pytest.raises(SerializationError, match="Failed to serialize object"):
            await serializer.serialize(user, schema_id)
    
    @pytest.mark.asyncio
    async def test_deserialization_error_handling(self, serializer):
        """Test deserialization error handling."""
        # Valid wire format but invalid Avro content
        message = b"\x00\x00\x00\x30\x39invalid avro data"
        
        serializer.client.get_schema_by_global_id.return_value = {"type": "string"}
        
        with pytest.raises(DeserializationError, match="Failed to deserialize message"):
            await serializer.deserialize(message, SimpleUser)
    
    
    
    
    
    
    
    @pytest.mark.asyncio
    async def test_serialize_auto_lookup_content_success(self, serializer, simple_user_schema):
        """Test serialize with automatic lookup using content search."""
        user = SimpleUser(name="Auto Serialize User", age=50)
        schema_id = 88888
        
        # Mock client methods for automatic lookup
        serializer.client.find_artifact_by_content.return_value = ("found-group", "found-artifact")
        serializer.client.get_latest_schema.return_value = (schema_id, simple_user_schema)
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema
        
        # Test automatic lookup (no schema_id provided)
        result = await serializer.serialize(user)
        
        assert isinstance(result, bytes)
        assert result[0] == 0x0  # Magic byte
        
        # Verify the lookup flow
        serializer.client.find_artifact_by_content.assert_called_once()
        serializer.client.get_latest_schema.assert_called_once_with("found-group", "found-artifact")
        serializer.client.get_schema_by_global_id.assert_called_once_with(schema_id)
    
    @pytest.mark.asyncio
    async def test_serialize_auto_lookup_fingerprint_fallback(self, serializer, simple_user_schema):
        """Test serialize with automatic lookup fallback to fingerprint search."""
        user = SimpleUser(name="Fingerprint User", age=55)
        schema_id = 77777
        
        # Mock content search to fail, fingerprint to succeed
        serializer.client.find_artifact_by_content.return_value = None
        serializer.client.find_artifact_by_fingerprint.return_value = ("fp-group", "fp-artifact")
        serializer.client.get_latest_schema.return_value = (schema_id, simple_user_schema)
        serializer.client.get_schema_by_global_id.return_value = simple_user_schema
        
        # Test automatic lookup with fingerprint fallback
        result = await serializer.serialize(user)
        
        assert isinstance(result, bytes)
        assert result[0] == 0x0  # Magic byte
        
        # Verify both methods were tried
        serializer.client.find_artifact_by_content.assert_called_once()
        serializer.client.find_artifact_by_fingerprint.assert_called_once()
        serializer.client.get_latest_schema.assert_called_once_with("fp-group", "fp-artifact")
    
    @pytest.mark.asyncio
    async def test_serialize_auto_lookup_no_match(self, serializer):
        """Test serialize automatic lookup when no matching schema found."""
        from avrocurio.exceptions import SchemaMatchError
        
        user = SimpleUser(name="No Match User", age=60)
        
        # Mock both search methods to return None
        serializer.client.find_artifact_by_content.return_value = None
        serializer.client.find_artifact_by_fingerprint.return_value = None
        
        # Test that SchemaMatchError is raised
        with pytest.raises(SchemaMatchError, match="No matching schema found in registry for automatic lookup"):
            await serializer.serialize(user)
    
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
        assert not hasattr(serializer.client, 'find_artifact_by_content') or \
               not serializer.client.find_artifact_by_content.called
    
    @pytest.mark.asyncio
    async def test_serialize_error_handling_enhanced(self, serializer):
        """Test enhanced error messages for different serialize scenarios."""
        user = SimpleUser(name="Error User", age=25)
        
        # Test error with explicit schema_id
        serializer.client.get_schema_by_global_id.side_effect = Exception("Registry error")
        
        with pytest.raises(SerializationError, match="Failed to serialize object with schema ID 12345"):
            await serializer.serialize(user, schema_id=12345)
        
        # Reset and test error with automatic lookup
        serializer.client.get_schema_by_global_id.side_effect = None
        serializer.client.find_artifact_by_content.return_value = ("found", "artifact")
        serializer.client.get_latest_schema.side_effect = Exception("Registry error")
        
        with pytest.raises(SerializationError, match="Failed to serialize object with automatic schema lookup"):
            await serializer.serialize(user)