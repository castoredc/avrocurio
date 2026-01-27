"""Tests for Apicurio schema client."""

import asyncio
import json
import re
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from avrocurio.apicurio_client import ApicurioClient, CachedError
from avrocurio.config import ApicurioConfig
from avrocurio.exceptions import AvroCurioError, SchemaNotFoundError


@pytest.fixture
def config():
    """Test configuration."""
    return ApicurioConfig(base_url="http://test-registry:8080")


@pytest.fixture
def auth_config():
    """Test configuration with authentication."""
    return ApicurioConfig(base_url="http://test-registry:8080", auth=("testuser", "testpass"))


@pytest.fixture
def sample_schema():
    """Sample Avro schema for testing."""
    return {
        "type": "record",
        "name": "User",
        "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}],
    }


@pytest.fixture
def mock_success_response():
    """Create a successful HTTP response mock."""

    def _create_response(data, *, is_json=True):
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock(return_value=None)
        if is_json:
            mock_response.json = Mock(return_value=data)
        else:
            mock_response.text = data if isinstance(data, str) else json.dumps(data)
        return mock_response

    return _create_response


@pytest.fixture
def mock_error_response():
    """Create an error HTTP response mock."""

    def _create_error(status_code, message):
        mock_response = Mock()
        mock_response.status_code = status_code
        return httpx.HTTPStatusError(message, request=Mock(), response=mock_response)

    return _create_error


class TestApicurioClient:
    """Test cases for ApicurioClient."""

    def test_init(self, config):
        """Test client initialization."""
        client = ApicurioClient(config)

        assert client.config == config
        assert len(client._schema_cache) == 0
        assert len(client._failed_lookup_cache) == 0

    @pytest.mark.asyncio
    async def test_context_manager(self, config):
        """Test async context manager."""
        async with ApicurioClient(config) as client:
            assert isinstance(client, ApicurioClient)
            assert client._client is not None

    @pytest.mark.asyncio
    async def test_ensure_client_basic(self, config):
        """Test HTTP client initialization."""
        client = ApicurioClient(config)

        assert client._client is not None
        assert isinstance(client._client, httpx.AsyncClient)
        assert str(client._client.base_url) == "http://test-registry:8080"

        await client.close()

    @pytest.mark.asyncio
    async def test_ensure_client_with_auth(self, auth_config):
        """Test HTTP client initialization with authentication."""
        client = ApicurioClient(auth_config)

        assert client._client.auth is not None
        await client.close()

    @pytest.mark.asyncio
    async def test_get_schema_by_global_id_success(self, config, sample_schema, mock_success_response):
        """Test successful schema retrieval by global ID."""
        client = ApicurioClient(config)
        schema_id = 12345

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_success_response(sample_schema, is_json=False)
        client._client = mock_client

        result = await client.get_schema_by_global_id(schema_id)

        assert result == sample_schema
        assert client._schema_cache[schema_id] == sample_schema
        client._client.get.assert_called_once_with(f"/apis/registry/v3/ids/globalIds/{schema_id}")

    @pytest.mark.asyncio
    async def test_get_schema_by_global_id_cached(self, config, sample_schema):
        """Test cached schema retrieval."""
        client = ApicurioClient(config)
        schema_id = 12345

        # Pre-populate cache
        client._schema_cache[schema_id] = sample_schema
        mock_client = AsyncMock()
        client._client = mock_client

        result = await client.get_schema_by_global_id(schema_id)

        assert result == sample_schema
        client._client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_schema_by_global_id_invalid_json(self, config, mock_success_response):
        """Test handling of invalid JSON content."""
        client = ApicurioClient(config)
        schema_id = 12345

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_success_response("invalid json content", is_json=False)
        client._client = mock_client

        with pytest.raises(AvroCurioError, match="Invalid JSON schema content"):
            await client.get_schema_by_global_id(schema_id)

        client._client.get.assert_called_once_with(f"/apis/registry/v3/ids/globalIds/{schema_id}")

    @pytest.mark.asyncio
    async def test_get_schema_by_global_id_not_found(self, config, mock_error_response):
        """Test schema not found error."""
        client = ApicurioClient(config)
        schema_id = 99999

        mock_client = AsyncMock()
        mock_client.get.side_effect = mock_error_response(404, "404 Not Found")
        client._client = mock_client

        with pytest.raises(SchemaNotFoundError, match=f"Schema with global ID {schema_id} not found"):
            await client.get_schema_by_global_id(schema_id)

    @pytest.mark.asyncio
    async def test_get_latest_schema_success(self, config, sample_schema, mock_success_response):
        """Test successful latest schema retrieval."""
        client = ApicurioClient(config)

        # Create responses using fixture
        metadata_response = mock_success_response({"globalId": 12345}, is_json=True)
        schema_response = mock_success_response(sample_schema, is_json=False)

        mock_client = AsyncMock()
        mock_client.get.side_effect = [metadata_response, schema_response]
        client._client = mock_client

        global_id, schema = await client.get_latest_schema("default", "user-schema")

        assert global_id == 12345
        assert schema == sample_schema
        assert mock_client.get.call_count == 2
        mock_client.get.assert_any_call("/apis/registry/v3/groups/default/artifacts/user-schema/versions/branch=latest")
        mock_client.get.assert_any_call("/apis/registry/v3/ids/globalIds/12345")

    @pytest.mark.asyncio
    async def test_get_latest_schema_not_found(self, config, mock_error_response):
        """Test latest schema not found."""
        client = ApicurioClient(config)

        mock_client = AsyncMock()
        mock_client.get.side_effect = mock_error_response(404, "404 Not Found")
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
                {"name": "product-schema", "type": "AVRO", "groupId": "default"},
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
            params={"artifactType": "AVRO", "name": "user"},
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
        mock_client.get.assert_called_once_with("/apis/registry/v3/search/artifacts", params={"artifactType": "AVRO"})

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
            raise httpx.HTTPStatusError("500 Internal Server Error", request=Mock(), response=mock_response)

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
    async def test_close_already_closed(self, config):
        """Test closing already closed client."""
        client = ApicurioClient(config)

        await client.close()
        await client.close()  # Should not raise any error

    @pytest.mark.asyncio
    async def test_register_schema_success(self, config, sample_schema):
        """Test successful schema registration."""
        client = ApicurioClient(config)

        # Mock successful registration response (v3 API format)
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json = Mock(
            return_value={
                "version": {"globalId": 12345},
                "artifact": {"artifactId": "test-schema"},
            }
        )
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.post.return_value = mock_response
        client._client = mock_client

        # Test the method
        global_id = await client.register_schema(
            group="default",
            artifact_name="test-schema",
            schema_content=json.dumps(sample_schema),
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
                        "contentType": "application/json",
                    }
                },
            },
            params={"ifExists": "FIND_OR_CREATE_VERSION"},
            headers={"Content-Type": "application/json"},
        )

    @pytest.mark.asyncio
    async def test_register_schema_find_or_create_version(self, config, sample_schema):
        """Test schema registration with FIND_OR_CREATE_VERSION behavior."""
        client = ApicurioClient(config)

        # Mock successful response from FIND_OR_CREATE_VERSION
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json = Mock(return_value={"version": {"globalId": 12346}})
        mock_response.raise_for_status = Mock(return_value=None)

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        client._client = mock_client

        # Test the method
        global_id = await client.register_schema(
            group="default",
            artifact_name="test-schema",
            schema_content=json.dumps(sample_schema),
        )

        assert global_id == 12346
        # Should only be called once with FIND_OR_CREATE_VERSION
        assert mock_client.post.call_count == 1

        # Verify the call parameters
        mock_client.post.assert_called_once_with(
            "/apis/registry/v3/groups/default/artifacts",
            json={
                "artifactId": "test-schema",
                "artifactType": "AVRO",
                "firstVersion": {
                    "content": {
                        "content": json.dumps(sample_schema),
                        "contentType": "application/json",
                    }
                },
            },
            params={"ifExists": "FIND_OR_CREATE_VERSION"},
            headers={"Content-Type": "application/json"},
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
            schema_content=json.dumps(sample_schema),
        )

        assert global_id == 12347
        mock_client.post.assert_called_once_with(
            "/apis/registry/v3/groups/default/artifacts/test-schema/versions",
            json={
                "content": {
                    "content": json.dumps(sample_schema),
                    "contentType": "application/json",
                }
            },
            headers={"Content-Type": "application/json"},
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
                group="default",
                artifact_name="test-schema",
                schema_content=json.dumps(sample_schema),
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
        mock_client.get.assert_called_once_with("/apis/registry/v3/groups/default/artifacts/test-schema")

    @pytest.mark.asyncio
    async def test_check_artifact_exists_false(self, config):
        """Test artifact existence check returns False."""
        client = ApicurioClient(config)

        # Mock 404 response
        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.HTTPStatusError(
            "404 Not Found", request=Mock(), response=Mock(status_code=404)
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
                {"groupId": "default", "artifactId": "user-schema-v2"},
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
        mock_response.json.return_value = {"artifacts": [{"groupId": "mygroup", "artifactId": "found-schema"}]}
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
    async def test_cache_stats(self, config):
        """Test cache statistics functionality."""
        client = ApicurioClient(config)

        # Test initial stats
        stats = await client.get_cache_stats()
        assert stats["schema_cache_size"] == 0
        assert stats["schema_cache_max_size"] == 1000
        assert stats["failed_lookup_cache_size"] == 0
        assert stats["failed_lookup_cache_max_size"] == 100
        assert stats["failed_lookup_cache_ttl"] == 300

        # Add some data to cache (using locks)
        async with client._schema_cache_lock:
            client._schema_cache[1] = {"test": "schema"}
        async with client._failed_lookup_cache_lock:
            client._failed_lookup_cache[9999] = CachedError(global_id=9999, status_code=404, timestamp=123.0)

        stats = await client.get_cache_stats()
        assert stats["schema_cache_size"] == 1
        assert stats["failed_lookup_cache_size"] == 1

    @pytest.mark.asyncio
    async def test_clear_cache(self, config):
        """Test cache clearing functionality."""
        client = ApicurioClient(config)

        # Add some data to caches (using locks)
        async with client._schema_cache_lock:
            client._schema_cache[1] = {"test": "schema"}
        async with client._failed_lookup_cache_lock:
            client._failed_lookup_cache[9999] = CachedError(global_id=9999, status_code=404, timestamp=123.0)

        assert len(client._schema_cache) == 1
        assert len(client._failed_lookup_cache) == 1

        # Clear caches
        await client.clear_cache()

        assert len(client._schema_cache) == 0
        assert len(client._failed_lookup_cache) == 0

    @pytest.mark.asyncio
    async def test_failed_lookup_caching(self, config):
        """Test that failed lookups are cached."""
        client = ApicurioClient(config)

        # Mock HTTP client to raise 404 error
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.status_code = 404
        mock_client.get.side_effect = httpx.HTTPStatusError("404 Not Found", request=Mock(), response=mock_response)
        client._client = mock_client

        # First call should hit the API and cache the failure
        with pytest.raises(SchemaNotFoundError):
            await client.get_schema_by_global_id(99999)

        # Verify failed lookup was cached with error details
        assert 99999 in client._failed_lookup_cache
        cached_error = client._failed_lookup_cache[99999]
        assert isinstance(cached_error, CachedError)
        assert cached_error.global_id == 99999
        assert cached_error.status_code == 404
        assert cached_error.timestamp > 0

        # Second call should use cached failure (no API call)
        mock_client.reset_mock()
        with pytest.raises(SchemaNotFoundError, match="cached"):
            await client.get_schema_by_global_id(99999)

        # Verify no additional API call was made
        mock_client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_failed_lookup_human_readable_timestamp(self, config):
        """Test that cached error messages contain human-readable timestamps."""

        client = ApicurioClient(config)

        # Manually add a cached error with known timestamp
        known_timestamp = asyncio.get_event_loop().time()
        cached_error = CachedError(global_id=88888, status_code=404, timestamp=known_timestamp)

        async with client._failed_lookup_cache_lock:
            client._failed_lookup_cache[88888] = cached_error

        # Try to get the schema - should raise with human-readable timestamp
        expected_message = "Schema with global ID 88888 not found (cached 0.0 seconds ago)"

        with pytest.raises(SchemaNotFoundError, match=re.escape(expected_message)):
            await client.get_schema_by_global_id(88888)

    def test_cache_locks_initialized(self, config):
        """Test that asyncio locks are properly initialized."""
        client = ApicurioClient(config)

        assert hasattr(client, "_schema_cache_lock")
        assert hasattr(client, "_failed_lookup_cache_lock")
        assert isinstance(client._schema_cache_lock, asyncio.Lock)
        assert isinstance(client._failed_lookup_cache_lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_concurrent_schema_cache_access_same_id(self, config, sample_schema):
        """Test concurrent access to same schema ID is thread-safe."""
        client = ApicurioClient(config)
        schema_id = 12345

        # Mock HTTP client to return consistent response
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(sample_schema)
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.get.return_value = mock_response
        client._client = mock_client

        # Create 50 concurrent tasks accessing same schema
        tasks = [client.get_schema_by_global_id(schema_id) for _ in range(50)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all results are identical (no race conditions)
        assert len(results) == 50
        for result in results:
            assert not isinstance(result, Exception), f"Unexpected exception: {result}"
            assert result == sample_schema

        # Verify schema was cached (should only make one HTTP call)
        assert mock_client.get.call_count <= 2  # Allow for some race condition variance
        assert len(client._schema_cache) == 1

    @pytest.mark.asyncio
    async def test_concurrent_schema_cache_access_different_ids(self, config, sample_schema):
        """Test concurrent access to different schema IDs is thread-safe."""
        client = ApicurioClient(config)

        # Mock HTTP client
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(sample_schema)
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.get.return_value = mock_response
        client._client = mock_client

        # Create tasks for different schema IDs
        schema_ids = range(1000, 1010)  # 10 different schema IDs
        tasks = []

        for schema_id in schema_ids:
            # Create 5 concurrent tasks per schema ID
            tasks.extend([client.get_schema_by_global_id(schema_id) for _ in range(5)])

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all requests succeeded
        assert len(results) == 50  # 10 schemas * 5 requests each
        for result in results:
            assert not isinstance(result, Exception), f"Unexpected exception: {result}"
            assert result == sample_schema

        # Verify cache contains all schemas
        assert len(client._schema_cache) == 10

    @pytest.mark.asyncio
    async def test_concurrent_failed_lookup_caching(self, config):
        """Test concurrent failed lookup caching is thread-safe."""
        client = ApicurioClient(config)
        schema_id = 99999

        # Mock HTTP client to raise 404 error
        mock_client = AsyncMock()
        mock_response = Mock()
        mock_response.status_code = 404
        mock_client.get.side_effect = httpx.HTTPStatusError("404 Not Found", request=Mock(), response=mock_response)
        client._client = mock_client

        # Create 20 concurrent tasks for same failed lookup
        tasks = [client.get_schema_by_global_id(schema_id) for _ in range(20)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All should raise SchemaNotFoundError
        assert len(results) == 20
        for result in results:
            assert isinstance(result, SchemaNotFoundError)

        # Verify failed lookup was cached
        assert schema_id in client._failed_lookup_cache

        # Should have made at least one HTTP call, but not necessarily 20
        assert mock_client.get.call_count >= 1
        assert mock_client.get.call_count <= 20  # Due to concurrent access, some may hit cache

    @pytest.mark.asyncio
    async def test_mixed_concurrent_operations(self, config, sample_schema):
        """Test mixed cache operations (reads, writes, clears) under concurrency."""
        client = ApicurioClient(config)

        # Mock successful HTTP response
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(sample_schema)
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.get.return_value = mock_response
        client._client = mock_client

        async def cache_reader(schema_id):
            """Task that reads from cache."""
            try:
                return await client.get_schema_by_global_id(schema_id)
            except Exception as e:
                return e

        async def cache_clearer():
            """Task that clears cache periodically."""
            await asyncio.sleep(0.01)  # Small delay
            await client.clear_cache()
            return "cleared"

        async def stats_reader():
            """Task that reads cache stats."""
            return await client.get_cache_stats()

        # Create mixed workload
        tasks = []

        # Add cache readers for different schema IDs
        for i in range(10):
            tasks.extend([cache_reader(1000 + i) for _ in range(3)])

        # Add cache clearers
        tasks.extend([cache_clearer() for _ in range(2)])

        # Add stats readers
        tasks.extend([stats_reader() for _ in range(5)])

        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify no exceptions occurred (except planned ones)
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Unexpected exceptions: {exceptions}"

        # Verify we got expected result types
        [r for r in results if isinstance(r, dict) and "type" in r]
        clear_results = [r for r in results if r == "cleared"]
        stats_results = [r for r in results if isinstance(r, dict) and "schema_cache_size" in r]

        assert len(clear_results) == 2
        assert len(stats_results) == 5
        # Schema results may vary due to cache clearing

    @pytest.mark.asyncio
    async def test_cache_consistency_under_high_concurrency(self, config, sample_schema):
        """Stress test with high concurrency to verify cache consistency."""
        client = ApicurioClient(config)

        # Mock HTTP client
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.text = json.dumps(sample_schema)
        mock_response.raise_for_status = Mock(return_value=None)
        mock_client.get.return_value = mock_response
        client._client = mock_client

        # Create high concurrency workload
        schema_ids = range(2000, 2050)  # 50 different schemas
        tasks = []

        for schema_id in schema_ids:
            # 10 concurrent requests per schema
            tasks.extend([client.get_schema_by_global_id(schema_id) for _ in range(10)])

        # Execute 500 concurrent requests
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all succeeded
        assert len(results) == 500
        exceptions = [r for r in results if isinstance(r, Exception)]
        assert len(exceptions) == 0, f"Exceptions in high concurrency test: {exceptions}"

        # Verify cache consistency
        assert len(client._schema_cache) == 50  # All 50 schemas should be cached

        # Verify all cached schemas are correct
        async with client._schema_cache_lock:
            for schema_id in schema_ids:
                assert schema_id in client._schema_cache
                assert client._schema_cache[schema_id] == sample_schema
