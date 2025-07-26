"""Integration test configuration and fixtures."""

import os
import uuid
import httpx
import pytest
from typing import AsyncGenerator

from avrocurio import ApicurioClient, ApicurioConfig, AvroSerializer


def pytest_configure(config):
    """Register integration test marker."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests requiring Apicurio"
    )


def pytest_runtest_setup(item):
    """Skip integration tests if Apicurio is not available."""
    if "integration" in [mark.name for mark in item.iter_markers()]:
        if not is_apicurio_available():
            pytest.skip("Apicurio Registry not available on localhost:8080")


def is_apicurio_available() -> bool:
    """Check if Apicurio Registry is running on localhost:8080."""
    try:
        import httpx
        response = httpx.get("http://localhost:8080/health", timeout=5.0)
        return response.status_code == 200
    except Exception:
        return False


@pytest.fixture
def apicurio_config() -> ApicurioConfig:
    """Configuration for local Apicurio Registry."""
    base_url = os.getenv("APICURIO_URL", "http://localhost:8080")
    return ApicurioConfig(base_url=base_url)


@pytest.fixture
async def apicurio_client(apicurio_config: ApicurioConfig) -> AsyncGenerator[ApicurioClient, None]:
    """Async Apicurio client for integration tests."""
    async with ApicurioClient(apicurio_config) as client:
        yield client


@pytest.fixture
async def serializer(apicurio_client: ApicurioClient) -> AvroSerializer:
    """Avro serializer for integration tests."""
    return AvroSerializer(apicurio_client)


@pytest.fixture
def test_group_id() -> str:
    """Unique group ID for test isolation."""
    return f"test-group-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def test_artifact_id() -> str:
    """Unique artifact ID for test isolation."""
    return f"test-event-{uuid.uuid4().hex[:8]}"