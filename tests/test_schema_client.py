"""Tests for Apicurio schema client."""

import json
import pytest
import httpx
from unittest.mock import AsyncMock, Mock

from avrocurio.schema_client import ApicurioClient
from avrocurio.config import ApicurioConfig
from avrocurio.exceptions import SchemaNotFoundError, AvroCurioError


@pytest.fixture
def config():
    """Test configuration."""
    return ApicurioConfig(base_url="http://test-registry:8080")


@pytest.fixture
def auth_config():
    """Test configuration with authentication."""
    return ApicurioConfig(
        base_url="http://test-registry:8080",
        auth=("testuser", "testpass")
    )


@pytest.fixture
def sample_schema():
    """Sample Avro schema for testing."""
    return {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }


class TestApicurioClient:
    """Test cases for ApicurioClient."""
    
    def test_init(self, config):
        """Test client initialization."""
        client = ApicurioClient(config)
        
        assert client.config == config
        assert client._client is None
        assert client._schema_cache == {}
    
    @pytest.mark.asyncio
    async def test_context_manager(self, config):
        """Test async context manager."""
        async with ApicurioClient(config) as client:
            assert isinstance(client, ApicurioClient)
            assert client._client is not None
        
        # Client should be closed after context
        assert client._client is None
    
    @pytest.mark.asyncio
    async def test_ensure_client_basic(self, config):
        """Test HTTP client initialization."""
        client = ApicurioClient(config)
        
        await client._ensure_client()
        
        assert client._client is not None
        assert isinstance(client._client, httpx.AsyncClient)
        assert str(client._client.base_url) == "http://test-registry:8080"
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_ensure_client_with_auth(self, auth_config):
        """Test HTTP client initialization with authentication."""
        client = ApicurioClient(auth_config)
        
        await client._ensure_client()
        
        assert client._client is not None
        assert client._client.auth is not None
        
        await client.close()
    
    @pytest.mark.asyncio
    async def test_get_schema_by_global_id_success(self, config, sample_schema):
        """Test successful schema retrieval by global ID."""
        client = ApicurioClient(config)
        
        # Mock the HTTP client
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(sample_schema)
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        # Test the method
        result = await client.get_schema_by_global_id(12345)
        
        assert result == sample_schema
        assert 12345 in client._schema_cache
        mock_client.get.assert_called_once_with("/apis/registry/v3/ids/globalIds/12345")
    
    @pytest.mark.asyncio
    async def test_get_schema_by_global_id_cached(self, config, sample_schema):
        """Test cached schema retrieval."""
        client = ApicurioClient(config)
        
        # Pre-populate cache
        client._schema_cache[12345] = sample_schema
        
        # Mock the HTTP client (should not be called)
        mock_client = AsyncMock()
        client._client = mock_client
        
        # Test the method
        result = await client.get_schema_by_global_id(12345)
        
        assert result == sample_schema
        mock_client.get.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_schema_by_global_id_not_found(self, config):
        """Test schema not found error."""
        client = ApicurioClient(config)
        
        # Mock 404 response
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 404
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        # Test the method
        with pytest.raises(SchemaNotFoundError, match="Schema with global ID 99999 not found"):
            await client.get_schema_by_global_id(99999)
    
    @pytest.mark.asyncio
    async def test_get_schema_by_global_id_raw_schema(self, config):
        """Test handling of raw schema content (non-JSON)."""
        client = ApicurioClient(config)
        
        raw_schema = 'not valid json schema content'
        
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.text = raw_schema
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        result = await client.get_schema_by_global_id(12345)
        
        # Should wrap raw content in schema field
        expected = {"schema": raw_schema}
        assert result == expected
    
    @pytest.mark.asyncio
    async def test_get_latest_schema_success(self, config, sample_schema):
        """Test successful latest schema retrieval."""
        client = ApicurioClient(config)
        
        # Mock metadata response
        metadata_response = AsyncMock()
        metadata_response.status_code = 200
        metadata_response.json = Mock(return_value={"globalId": 12345})
        metadata_response.raise_for_status = Mock(return_value=None)
        
        # Mock schema content response
        schema_response = AsyncMock()
        schema_response.status_code = 200
        schema_response.text = json.dumps(sample_schema)
        schema_response.raise_for_status = Mock(return_value=None)
        
        mock_client = AsyncMock()
        mock_client.get.side_effect = [metadata_response, schema_response]
        client._client = mock_client
        
        # Test the method
        global_id, schema = await client.get_latest_schema("default", "user-schema")
        
        assert global_id == 12345
        assert schema == sample_schema
        
        # Verify calls
        assert mock_client.get.call_count == 2
        mock_client.get.assert_any_call(
            "/apis/registry/v3/groups/default/artifacts/user-schema/versions/branch=latest"
        )
        mock_client.get.assert_any_call("/apis/registry/v3/ids/globalIds/12345")
    
    @pytest.mark.asyncio
    async def test_get_latest_schema_not_found(self, config):
        """Test latest schema not found."""
        client = ApicurioClient(config)
        
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 404
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        with pytest.raises(SchemaNotFoundError, match="Schema default/user-schema not found"):
            await client.get_latest_schema("default", "user-schema")
    
    @pytest.mark.asyncio
    async def test_search_artifacts_success(self, config):
        """Test successful artifact search."""
        client = ApicurioClient(config)
        
        search_results = {
            "artifacts": [
                {"name": "user-schema", "type": "AVRO", "groupId": "default"},
                {"name": "product-schema", "type": "AVRO", "groupId": "default"}
            ]
        }
        
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json = Mock(return_value=search_results)
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        # Test the method
        result = await client.search_artifacts(name="user", artifact_type="AVRO")
        
        assert result == search_results["artifacts"]
        mock_client.get.assert_called_once_with(
            "/apis/registry/v3/search/artifacts",
            params={"artifactType": "AVRO", "name": "user"}
        )
    
    @pytest.mark.asyncio
    async def test_search_artifacts_no_name_filter(self, config):
        """Test artifact search without name filter."""
        client = ApicurioClient(config)
        
        search_results = {"artifacts": []}
        
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json = Mock(return_value=search_results)
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        # Test the method
        result = await client.search_artifacts(artifact_type="AVRO")
        
        assert result == []
        mock_client.get.assert_called_once_with(
            "/apis/registry/v3/search/artifacts",
            params={"artifactType": "AVRO"}
        )
    
    @pytest.mark.asyncio
    async def test_http_error_handling(self, config):
        """Test HTTP error handling."""
        client = ApicurioClient(config)
        
        # Mock 500 error
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        def raise_error():
            raise httpx.HTTPStatusError(
                "500 Internal Server Error", request=None, response=mock_response
            )
        mock_response.raise_for_status = Mock(side_effect=raise_error)
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        with pytest.raises(AvroCurioError, match="HTTP error 500"):
            await client.get_schema_by_global_id(12345)
    
    @pytest.mark.asyncio
    async def test_request_error_handling(self, config):
        """Test request error handling."""
        client = ApicurioClient(config)
        
        # Mock request error
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.RequestError("Connection failed")
        client._client = mock_client
        
        with pytest.raises(AvroCurioError, match="Request error"):
            await client.get_schema_by_global_id(12345)
    
    @pytest.mark.asyncio
    async def test_close_client(self, config):
        """Test client cleanup."""
        client = ApicurioClient(config)
        
        # Initialize client
        await client._ensure_client()
        assert client._client is not None
        
        # Close client
        await client.close()
        assert client._client is None
    
    @pytest.mark.asyncio
    async def test_close_already_closed(self, config):
        """Test closing already closed client."""
        client = ApicurioClient(config)
        
        # Should not raise error
        await client.close()
        assert client._client is None
    
    @pytest.mark.asyncio
    async def test_register_schema_success(self, config, sample_schema):
        """Test successful schema registration."""
        client = ApicurioClient(config)
        
        # Mock successful registration response (v3 API format)
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json = Mock(return_value={
            "version": {"globalId": 12345},
            "artifact": {"artifactId": "test-schema"}
        })
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.post.return_value = mock_response
        client._client = mock_client
        
        # Test the method
        global_id = await client.register_schema(
            group_id="default",
            artifact_id="test-schema",
            schema_content=json.dumps(sample_schema)
        )
        
        assert global_id == 12345
        mock_client.post.assert_called_once_with(
            "/apis/registry/v3/groups/default/artifacts",
            json={
                "artifactId": "test-schema",
                "artifactType": "AVRO",
                "firstVersion": {
                    "content": {
                        "content": json.dumps(sample_schema),
                        "contentType": "application/json"
                    }
                }
            },
            params={"ifExists": "CREATE_VERSION"},
            headers={"Content-Type": "application/json"}
        )
    
    @pytest.mark.asyncio
    async def test_register_schema_conflict_creates_version(self, config, sample_schema):
        """Test schema registration with conflict creates new version."""
        client = ApicurioClient(config)
        
        # Mock conflict response for initial registration
        mock_conflict_response = AsyncMock()
        mock_conflict_response.status_code = 409
        
        # Mock successful version creation response
        mock_version_response = AsyncMock()
        mock_version_response.status_code = 200
        mock_version_response.json = Mock(return_value={"globalId": 12346})
        mock_version_response.raise_for_status = Mock(return_value=None)
        
        mock_client = AsyncMock()
        mock_client.post.side_effect = [
            mock_conflict_response,  # First call returns conflict
            mock_version_response    # Second call (version creation) succeeds
        ]
        client._client = mock_client
        
        # Test the method
        global_id = await client.register_schema(
            group_id="default",
            artifact_id="test-schema",
            schema_content=json.dumps(sample_schema)
        )
        
        assert global_id == 12346
        assert mock_client.post.call_count == 2
        
        # Verify version creation call (v3 API format)
        mock_client.post.assert_any_call(
            "/apis/registry/v3/groups/default/artifacts/test-schema/versions",
            json={
                "content": {
                    "content": json.dumps(sample_schema),
                    "contentType": "application/json"
                }
            },
            headers={"Content-Type": "application/json"}
        )
    
    @pytest.mark.asyncio
    async def test_register_schema_version_success(self, config, sample_schema):
        """Test successful schema version registration."""
        client = ApicurioClient(config)
        
        # Mock successful version registration response
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json = Mock(return_value={"globalId": 12347})
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.post.return_value = mock_response
        client._client = mock_client
        
        # Test the method
        global_id = await client.register_schema_version(
            group_id="default",
            artifact_id="test-schema",
            schema_content=json.dumps(sample_schema)
        )
        
        assert global_id == 12347
        mock_client.post.assert_called_once_with(
            "/apis/registry/v3/groups/default/artifacts/test-schema/versions",
            json={
                "content": {
                    "content": json.dumps(sample_schema),
                    "contentType": "application/json"
                }
            },
            headers={"Content-Type": "application/json"}
        )
    
    @pytest.mark.asyncio
    async def test_register_schema_no_global_id_error(self, config, sample_schema):
        """Test error when registration response has no global ID."""
        client = ApicurioClient(config)
        
        # Mock response without global ID (v3 API format)
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json = Mock(return_value={"version": {}})  # No globalId field in version
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.post.return_value = mock_response
        client._client = mock_client
        
        # Test the method
        with pytest.raises(AvroCurioError, match="No global ID returned for registered schema"):
            await client.register_schema(
                group_id="default",
                artifact_id="test-schema",
                schema_content=json.dumps(sample_schema)
            )
    
    @pytest.mark.asyncio
    async def test_check_artifact_exists_true(self, config):
        """Test artifact existence check returns True."""
        client = ApicurioClient(config)
        
        # Mock successful artifact retrieval
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        # Test the method
        exists = await client.check_artifact_exists("default", "test-schema")
        
        assert exists is True
        mock_client.get.assert_called_once_with(
            "/apis/registry/v3/groups/default/artifacts/test-schema"
        )
    
    @pytest.mark.asyncio
    async def test_check_artifact_exists_false(self, config):
        """Test artifact existence check returns False."""
        client = ApicurioClient(config)
        
        # Mock 404 response
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.HTTPStatusError(
            "404 Not Found", request=None, response=Mock(status_code=404)
        )
        client._client = mock_client
        
        # Test the method
        exists = await client.check_artifact_exists("default", "nonexistent-schema")
        
        assert exists is False
    
    @pytest.mark.asyncio
    async def test_find_artifact_by_content_success(self, config, sample_schema):
        """Test successful content-based artifact search."""
        client = ApicurioClient(config)
        
        # Mock response for content search
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "artifacts": [
                {"groupId": "default", "artifactId": "user-schema"},
                {"groupId": "default", "artifactId": "user-schema-v2"}
            ]
        }
        mock_response.raise_for_status.return_value = None
        
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        client._client = mock_client
        
        # Test the method
        schema_content = json.dumps(sample_schema)
        result = await client.find_artifact_by_content(schema_content, "default")
        
        assert result == ("default", "user-schema")
        
        # Verify API call
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert "/apis/registry/v3/search/artifacts" in call_args[0][0]
        assert call_args[1]["params"]["canonical"] == "true"
        assert call_args[1]["params"]["artifactType"] == "AVRO"
        assert call_args[1]["params"]["groupId"] == "default"
    
    @pytest.mark.asyncio
    async def test_find_artifact_by_content_no_group_filter(self, config, sample_schema):
        """Test content search without group filtering."""
        client = ApicurioClient(config)
        
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "artifacts": [{"groupId": "mygroup", "artifactId": "found-schema"}]
        }
        mock_response.raise_for_status.return_value = None
        
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        client._client = mock_client
        
        # Test without group_id filter
        schema_content = json.dumps(sample_schema)
        result = await client.find_artifact_by_content(schema_content)
        
        assert result == ("mygroup", "found-schema")
        
        # Verify no groupId param was sent
        call_args = mock_client.post.call_args
        assert "groupId" not in call_args[1]["params"]
    
    @pytest.mark.asyncio
    async def test_find_artifact_by_content_no_match(self, config, sample_schema):
        """Test content search with no matching artifacts."""
        client = ApicurioClient(config)
        
        # Mock empty response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"artifacts": []}
        mock_response.raise_for_status.return_value = None
        
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        client._client = mock_client
        
        # Test the method
        schema_content = json.dumps(sample_schema)
        result = await client.find_artifact_by_content(schema_content, "default")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_find_artifact_by_fingerprint_success(self, config, sample_schema):
        """Test successful fingerprint-based artifact search."""
        client = ApicurioClient(config)
        
        # Mock search response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "artifacts": [
                {"groupId": "default", "artifactId": "user-schema"},
                {"groupId": "default", "artifactId": "other-schema"}
            ]
        }
        mock_response.raise_for_status.return_value = None
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        # Mock get_latest_schema for the matching artifact
        client.get_latest_schema = AsyncMock()
        client.get_latest_schema.return_value = (123, {"schema": json.dumps(sample_schema)})
        
        # Test the method
        schema_content = json.dumps(sample_schema)
        result = await client.find_artifact_by_fingerprint(schema_content, "default")
        
        assert result == ("default", "user-schema")
        
        # Verify get_latest_schema was called for the first artifact
        client.get_latest_schema.assert_called_with("default", "user-schema")
    
    @pytest.mark.asyncio
    async def test_find_artifact_by_fingerprint_no_match(self, config, sample_schema):
        """Test fingerprint search with no matching schemas."""
        client = ApicurioClient(config)
        
        # Different schema for comparison
        different_schema = {
            "type": "record",
            "name": "Product", 
            "fields": [{"name": "id", "type": "string"}]
        }
        
        # Mock search response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "artifacts": [{"groupId": "default", "artifactId": "other-schema"}]
        }
        mock_response.raise_for_status.return_value = None
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        # Mock get_latest_schema returning different schema
        client.get_latest_schema = AsyncMock()
        client.get_latest_schema.return_value = (456, {"schema": json.dumps(different_schema)})
        
        # Test the method
        schema_content = json.dumps(sample_schema)
        result = await client.find_artifact_by_fingerprint(schema_content, "default")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_find_artifact_by_fingerprint_no_group_filter(self, config, sample_schema):
        """Test fingerprint search without group filtering."""
        client = ApicurioClient(config)
        
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "artifacts": [{"groupId": "anygroup", "artifactId": "match-schema"}]
        }
        mock_response.raise_for_status.return_value = None
        
        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        client._client = mock_client
        
        # Mock matching schema
        client.get_latest_schema = AsyncMock()
        client.get_latest_schema.return_value = (789, {"schema": json.dumps(sample_schema)})
        
        # Test without group_id filter
        schema_content = json.dumps(sample_schema)
        result = await client.find_artifact_by_fingerprint(schema_content)
        
        assert result == ("anygroup", "match-schema")
        
        # Verify no groupId param was sent
        call_args = mock_client.get.call_args
        assert "groupId" not in call_args[1]["params"]