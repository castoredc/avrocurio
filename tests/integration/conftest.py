"""Integration test configuration and fixtures."""

import os
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from avrocurio import ApicurioClient, ApicurioConfig, AvroSerializer


def pytest_configure(config):
    """Register integration test marker."""
    config.addinivalue_line("markers", "integration: marks tests as integration tests requiring Apicurio")


DEFAULT_APICURIO_URL = "http://localhost:8080"


def apicurio_base_url() -> str:
    """Base URL of the Apicurio Registry to test against."""
    return os.getenv("APICURIO_URL", DEFAULT_APICURIO_URL)


def pytest_runtest_setup(item):
    """Skip integration tests with an expect-fail when Apicurio is not available."""
    if "integration" in [mark.name for mark in item.iter_markers()] and not is_apicurio_available():
        pytest.xfail(f"Apicurio Registry not available on {apicurio_base_url()}")


def is_apicurio_available() -> bool:
    """Check if the Apicurio Registry API is reachable.

    Probes the v3 API rather than /health: the health endpoints are served on the
    separate Quarkus management port, not on the API port we actually talk to.
    """
    try:
        response = httpx.get(f"{apicurio_base_url()}/apis/registry/v3/system/info", timeout=5.0)
    except Exception:
        return False
    else:
        return response.status_code == 200


@pytest.fixture
def apicurio_config() -> ApicurioConfig:
    """Configuration for local Apicurio Registry."""
    return ApicurioConfig(base_url=apicurio_base_url())


@pytest.fixture
async def apicurio_client(apicurio_config: ApicurioConfig) -> AsyncGenerator[ApicurioClient]:
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
