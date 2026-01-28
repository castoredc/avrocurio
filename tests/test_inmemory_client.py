"""Tests for InMemoryClient."""

import asyncio
import json

import pytest

from avrocurio import InMemoryClient
from avrocurio.exceptions import AvroCurioError, SchemaNotFoundError
from avrocurio.serializer import AvroSerializer
from tests.test_schemas import ComplexUser, SimpleUser


@pytest.fixture
def client() -> InMemoryClient:
    """Create a fresh InMemoryClient for each test."""
    return InMemoryClient()


@pytest.fixture
def sample_schema() -> str:
    """Sample Avro schema for testing."""
    return json.dumps(
        {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
            ],
        }
    )


@pytest.fixture
def sample_schema_variant() -> str:
    """Same schema with different formatting."""
    return json.dumps(
        {
            "name": "TestRecord",
            "type": "record",
            "fields": [
                {"type": "int", "name": "id"},
                {"type": "string", "name": "name"},
            ],
        }
    )


# Basic Schema Operations


@pytest.mark.asyncio
async def test_get_schema_by_global_id_success(client: InMemoryClient, sample_schema: str) -> None:
    """Test retrieving an existing schema by global ID."""
    # Register schema first
    global_id = await client.register_schema("default", "test-schema", sample_schema)

    # Retrieve it
    schema = await client.get_schema_by_global_id(global_id)
    assert schema == json.loads(sample_schema)


@pytest.mark.asyncio
async def test_get_schema_by_global_id_not_found(client: InMemoryClient) -> None:
    """Test that SchemaNotFoundError is raised for non-existent ID."""
    with pytest.raises(SchemaNotFoundError) as exc_info:
        await client.get_schema_by_global_id(999)

    assert "Schema with global ID 999 not found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_register_schema_new(client: InMemoryClient, sample_schema: str) -> None:
    """Test registering a new schema returns a global ID."""
    global_id = await client.register_schema("default", "test-schema", sample_schema)

    assert global_id == 1
    assert await client.check_artifact_exists("default", "test-schema")


@pytest.mark.asyncio
async def test_register_schema_idempotent(client: InMemoryClient, sample_schema: str) -> None:
    """Test that registering the same schema returns the same ID."""
    global_id_1 = await client.register_schema("default", "test-schema", sample_schema)
    global_id_2 = await client.register_schema("default", "test-schema", sample_schema)

    assert global_id_1 == global_id_2


@pytest.mark.asyncio
async def test_register_schema_normalized(
    client: InMemoryClient, sample_schema: str, sample_schema_variant: str
) -> None:
    """Test that schemas with different formatting but same content get same ID."""
    global_id_1 = await client.register_schema("default", "test-schema", sample_schema)
    global_id_2 = await client.register_schema("default", "test-schema-2", sample_schema_variant)

    assert global_id_1 == global_id_2


# Version Management


@pytest.mark.asyncio
async def test_register_schema_creates_artifact(client: InMemoryClient, sample_schema: str) -> None:
    """Test that first registration creates an artifact."""
    await client.register_schema("default", "test-schema", sample_schema)

    exists = await client.check_artifact_exists("default", "test-schema")
    assert exists


@pytest.mark.asyncio
async def test_register_schema_version_new(client: InMemoryClient, sample_schema: str) -> None:
    """Test adding a new version to an existing artifact."""
    # Register initial schema
    await client.register_schema("default", "test-schema", sample_schema)

    # Register a new version with different schema
    new_schema = json.dumps(
        {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
            ],
        }
    )
    global_id = await client.register_schema_version("default", "test-schema", new_schema)

    # Should get a new global ID
    assert global_id == 2

    # Latest version should be the new one
    latest_id, latest_schema = await client.get_latest_schema("default", "test-schema")
    assert latest_id == global_id
    assert latest_schema == json.loads(new_schema)


@pytest.mark.asyncio
async def test_register_schema_version_artifact_not_exists(client: InMemoryClient, sample_schema: str) -> None:
    """Test that registering version for non-existent artifact raises error."""
    with pytest.raises(AvroCurioError) as exc_info:
        await client.register_schema_version("default", "non-existent", sample_schema)

    assert "Artifact default/non-existent does not exist" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_latest_schema(client: InMemoryClient, sample_schema: str) -> None:
    """Test getting the latest version of a schema."""
    global_id = await client.register_schema("default", "test-schema", sample_schema)

    latest_id, latest_schema = await client.get_latest_schema("default", "test-schema")
    assert latest_id == global_id
    assert latest_schema == json.loads(sample_schema)


@pytest.mark.asyncio
async def test_get_latest_schema_not_found(client: InMemoryClient) -> None:
    """Test that get_latest_schema raises error for non-existent artifact."""
    with pytest.raises(SchemaNotFoundError) as exc_info:
        await client.get_latest_schema("default", "non-existent")

    assert "Schema default/non-existent not found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_latest_schema_returns_most_recent(client: InMemoryClient, sample_schema: str) -> None:
    """Test that get_latest_schema returns the most recent version."""
    # Register initial version
    await client.register_schema("default", "test-schema", sample_schema)

    # Register second version
    new_schema = json.dumps(
        {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
            ],
        }
    )
    new_id = await client.register_schema_version("default", "test-schema", new_schema)

    # Latest should be the second version
    latest_id, latest_schema = await client.get_latest_schema("default", "test-schema")
    assert latest_id == new_id
    assert latest_schema == json.loads(new_schema)


# Content-Based Search


@pytest.mark.asyncio
async def test_find_artifact_by_content_with_match(client: InMemoryClient, sample_schema: str) -> None:
    """Test finding an artifact by its schema content."""
    await client.register_schema("default", "test-schema", sample_schema)

    result = await client.find_artifact_by_content(sample_schema)
    assert result == ("default", "test-schema")


@pytest.mark.asyncio
async def test_find_artifact_by_content_no_match(client: InMemoryClient) -> None:
    """Test that find_artifact_by_content returns None if not found."""
    schema = json.dumps({"type": "record", "name": "Test", "fields": []})
    result = await client.find_artifact_by_content(schema)
    assert result is None


@pytest.mark.asyncio
async def test_find_artifact_by_content_with_group_filter(client: InMemoryClient, sample_schema: str) -> None:
    """Test filtering by group_id when finding artifacts."""
    # Register in two different groups
    await client.register_schema("group1", "test-schema", sample_schema)
    await client.register_schema("group2", "test-schema", sample_schema)

    # Search with group filter
    result = await client.find_artifact_by_content(sample_schema, group_id="group1")
    assert result == ("group1", "test-schema")


@pytest.mark.asyncio
async def test_find_artifact_by_content_normalized(
    client: InMemoryClient, sample_schema: str, sample_schema_variant: str
) -> None:
    """Test that find_artifact_by_content matches despite formatting differences."""
    await client.register_schema("default", "test-schema", sample_schema)

    # Search with differently formatted but equivalent schema
    result = await client.find_artifact_by_content(sample_schema_variant)
    assert result == ("default", "test-schema")


# Artifact Search


@pytest.mark.asyncio
async def test_search_artifacts_all(client: InMemoryClient, sample_schema: str) -> None:
    """Test searching for all artifacts."""
    await client.register_schema("default", "schema-1", sample_schema)

    # Register a different schema for schema-2
    schema2 = json.dumps(
        {
            "type": "record",
            "name": "DifferentRecord",
            "fields": [
                {"name": "value", "type": "string"},
            ],
        }
    )
    await client.register_schema("default", "schema-2", schema2)

    results = await client.search_artifacts()
    assert len(results) == 2
    assert any(r["artifactId"] == "schema-1" for r in results)
    assert any(r["artifactId"] == "schema-2" for r in results)


@pytest.mark.asyncio
async def test_search_artifacts_with_name_filter(client: InMemoryClient, sample_schema: str) -> None:
    """Test filtering artifacts by name."""
    await client.register_schema("default", "user-schema", sample_schema)
    await client.register_schema("default", "product-schema", sample_schema)

    results = await client.search_artifacts(name="user")
    assert len(results) == 1
    assert results[0]["artifactId"] == "user-schema"


@pytest.mark.asyncio
async def test_search_artifacts_with_type_filter(client: InMemoryClient, sample_schema: str) -> None:
    """Test filtering artifacts by type."""
    await client.register_schema("default", "test-schema", sample_schema)

    results = await client.search_artifacts(artifact_type="AVRO")
    assert len(results) == 1
    assert results[0]["artifactType"] == "AVRO"


@pytest.mark.asyncio
async def test_check_artifact_exists_true(client: InMemoryClient, sample_schema: str) -> None:
    """Test check_artifact_exists returns True for existing artifact."""
    await client.register_schema("default", "test-schema", sample_schema)

    exists = await client.check_artifact_exists("default", "test-schema")
    assert exists


@pytest.mark.asyncio
async def test_check_artifact_exists_false(client: InMemoryClient) -> None:
    """Test check_artifact_exists returns False for non-existent artifact."""
    exists = await client.check_artifact_exists("default", "non-existent")
    assert not exists


# Helper Methods


@pytest.mark.asyncio
async def test_add_schema_helper(client: InMemoryClient) -> None:
    """Test the add_schema helper method for pre-populating schemas."""
    schema_dict = {"type": "record", "name": "Test", "fields": []}

    await client.add_schema(100, schema_dict, "default", "test-schema")

    # Should be able to retrieve it
    schema = await client.get_schema_by_global_id(100)
    assert schema == schema_dict

    # Should exist as artifact
    exists = await client.check_artifact_exists("default", "test-schema")
    assert exists


@pytest.mark.asyncio
async def test_reset_helper(client: InMemoryClient, sample_schema: str) -> None:
    """Test the reset helper method clears all state."""
    await client.register_schema("default", "test-schema", sample_schema)

    await client.reset()

    # Should not exist anymore
    exists = await client.check_artifact_exists("default", "test-schema")
    assert not exists

    # Next schema should get ID 1 again
    new_id = await client.register_schema("default", "new-schema", sample_schema)
    assert new_id == 1


# Context Manager


@pytest.mark.asyncio
async def test_context_manager() -> None:
    """Test async context manager protocol."""
    async with InMemoryClient() as client:
        schema = json.dumps({"type": "record", "name": "Test", "fields": []})
        global_id = await client.register_schema("default", "test-schema", schema)
        assert global_id == 1


# Integration with AvroSerializer


@pytest.mark.asyncio
async def test_serializer_with_inmemory_explicit_id() -> None:
    """Test AvroSerializer with InMemoryClient using explicit schema ID."""
    client = InMemoryClient()
    serializer = AvroSerializer(client)

    user = SimpleUser(name="Alice", age=30)

    # Register schema
    schema_id = await client.register_schema("default", "user-schema", user.avro_schema())

    # Serialize with explicit schema ID
    message = await serializer.serialize(user, schema_id)

    # Should start with magic byte and schema ID
    assert len(message) > 5
    assert message[0] == 0  # Magic byte


@pytest.mark.asyncio
async def test_serializer_with_inmemory_auto_lookup() -> None:
    """Test AvroSerializer with InMemoryClient using automatic schema lookup."""
    client = InMemoryClient()
    serializer = AvroSerializer(client)

    user = SimpleUser(name="Bob", age=25)

    # Register schema
    await client.register_schema("default", "user-schema", user.avro_schema())

    # Serialize without schema ID (automatic lookup)
    message = await serializer.serialize(user)

    # Should succeed
    assert len(message) > 5


@pytest.mark.asyncio
async def test_serializer_with_inmemory_round_trip() -> None:
    """Test full serialize/deserialize cycle with InMemoryClient."""
    client = InMemoryClient()
    serializer = AvroSerializer(client)

    user = ComplexUser(name="Charlie", age=35, email="charlie@example.com", is_active=True)

    # Register schema
    schema_id = await client.register_schema("default", "user-schema", user.avro_schema())

    # Serialize
    message = await serializer.serialize(user, schema_id)

    # Deserialize
    result = await serializer.deserialize(message, ComplexUser)

    # Verify round-trip
    assert result.name == user.name
    assert result.age == user.age
    assert result.email == user.email
    assert result.is_active == user.is_active


# API Compatibility


@pytest.mark.asyncio
async def test_clear_cache_noop(client: InMemoryClient, sample_schema: str) -> None:
    """Test that clear_cache is a no-op but doesn't error."""
    global_id = await client.register_schema("default", "test-schema", sample_schema)

    await client.clear_cache()

    # Schema should still be retrievable (no actual cache to clear)
    schema = await client.get_schema_by_global_id(global_id)
    assert schema == json.loads(sample_schema)


@pytest.mark.asyncio
async def test_get_cache_stats_returns_expected_format(client: InMemoryClient, sample_schema: str) -> None:
    """Test that get_cache_stats returns dict with expected keys."""
    await client.register_schema("default", "test-schema", sample_schema)

    stats = await client.get_cache_stats()

    # Should have all expected keys
    assert "schema_cache_size" in stats
    assert "schema_cache_max_size" in stats
    assert "failed_lookup_cache_size" in stats
    assert "failed_lookup_cache_max_size" in stats
    assert "failed_lookup_cache_ttl" in stats

    # Schema count should reflect actual schemas
    assert stats["schema_cache_size"] == 1


# Edge Cases


@pytest.mark.asyncio
async def test_multiple_groups_same_artifact_name(client: InMemoryClient, sample_schema: str) -> None:
    """Test that same artifact name can exist in different groups with same schema."""
    id1 = await client.register_schema("group1", "shared-name", sample_schema)
    id2 = await client.register_schema("group2", "shared-name", sample_schema)

    # Same schema content should get same global ID
    assert id1 == id2

    # Both artifacts should exist
    assert await client.check_artifact_exists("group1", "shared-name")
    assert await client.check_artifact_exists("group2", "shared-name")

    # Both should return the same schema
    schema1 = await client.get_schema_by_global_id(id1)
    schema2 = await client.get_schema_by_global_id(id2)
    assert schema1 == schema2


@pytest.mark.asyncio
async def test_register_schema_version_with_duplicate_content(client: InMemoryClient, sample_schema: str) -> None:
    """Test that registering a version with duplicate content returns existing ID."""
    # Register initial version
    id1 = await client.register_schema("default", "test-schema", sample_schema)

    # Try to register same content as new version
    id2 = await client.register_schema_version("default", "test-schema", sample_schema)

    # Should return same ID
    assert id1 == id2


@pytest.mark.asyncio
async def test_empty_search_results(client: InMemoryClient) -> None:
    """Test that search returns empty list when no artifacts exist."""
    results = await client.search_artifacts()
    assert results == []


@pytest.mark.asyncio
async def test_search_with_no_matches(client: InMemoryClient, sample_schema: str) -> None:
    """Test that search with non-matching filter returns empty list."""
    await client.register_schema("default", "test-schema", sample_schema)

    results = await client.search_artifacts(name="nonexistent")
    assert results == []


# Concurrency Tests


@pytest.mark.asyncio
async def test_concurrent_register_schema() -> None:
    """Test that concurrent register_schema calls don't corrupt state."""
    client = InMemoryClient()

    # Create multiple different schemas
    schemas = [
        json.dumps({"type": "record", "name": f"Record{i}", "fields": [{"name": "value", "type": "int"}]})
        for i in range(10)
    ]

    # Register them all concurrently
    tasks = [client.register_schema("default", f"schema-{i}", schema) for i, schema in enumerate(schemas)]

    results = await asyncio.gather(*tasks)

    # All should succeed and get unique IDs
    assert len(results) == 10
    assert len(set(results)) == 10  # All IDs should be unique
    assert results == list(range(1, 11))  # Should be sequential


@pytest.mark.asyncio
async def test_concurrent_register_same_schema() -> None:
    """Test that concurrent registrations of same schema are idempotent."""
    client = InMemoryClient()

    schema = json.dumps({"type": "record", "name": "Test", "fields": []})

    # Try to register the same schema 10 times concurrently
    tasks = [client.register_schema("default", "test-schema", schema) for _ in range(10)]

    results = await asyncio.gather(*tasks)

    # All should return the same ID (idempotency)
    assert len(set(results)) == 1
    assert results[0] == 1


@pytest.mark.asyncio
async def test_concurrent_read_write() -> None:
    """Test that concurrent reads and writes work correctly."""
    client = InMemoryClient()

    # Pre-populate some schemas
    for i in range(5):
        schema = json.dumps({"type": "record", "name": f"Record{i}", "fields": []})
        await client.register_schema("default", f"schema-{i}", schema)

    # Mix of reads and writes
    async def read_task(schema_id: int) -> dict:
        return await client.get_schema_by_global_id(schema_id)

    async def write_task(i: int) -> int:
        schema = json.dumps({"type": "record", "name": f"NewRecord{i}", "fields": []})
        return await client.register_schema("default", f"new-schema-{i}", schema)

    # Run concurrent reads and writes
    tasks = []
    for i in range(5):
        tasks.append(read_task(i + 1))
        tasks.append(write_task(i))

    results = await asyncio.gather(*tasks)

    # Verify reads succeeded (returns dicts) and writes succeeded (returns ints)
    read_results = [r for r in results if isinstance(r, dict)]
    write_results = [r for r in results if isinstance(r, int)]

    assert len(read_results) == 5
    assert len(write_results) == 5
    assert all(isinstance(r, dict) for r in read_results)
    assert all(isinstance(r, int) for r in write_results)
