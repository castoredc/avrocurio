"""Tests for the main module imports and convenience functions."""

import pytest

import avrocurio
from avrocurio import (
    ApicurioClient,
    ApicurioConfig,
    AvroCurioError,
    AvroSerializer,
    DeserializationError,
    InvalidWireFormatError,
    SchemaMismatchError,
    SchemaNotFoundError,
    SerializationError,
    create_serializer,
)


class TestModuleImports:
    """Test that all expected exports are available."""

    def test_all_exports_available(self):
        """Test that __all__ exports are all importable."""
        expected_exports = [
            "AvroSerializer",
            "ApicurioClient",
            "ApicurioConfig",
            "AvroCurioError",
            "SchemaNotFoundError",
            "InvalidWireFormatError",
            "SerializationError",
            "DeserializationError",
            "SchemaMismatchError",
            "create_serializer",
        ]

        for export_name in expected_exports:
            assert hasattr(avrocurio, export_name)
            assert export_name in avrocurio.__all__

    def test_version_available(self):
        """Test that version is available."""
        assert hasattr(avrocurio, "__version__")
        assert isinstance(avrocurio.__version__, str)
        assert avrocurio.__version__ == "0.1.0"

    def test_class_imports(self):
        """Test that imported classes are the correct types."""
        assert isinstance(AvroSerializer, type)
        assert isinstance(ApicurioClient, type)
        assert isinstance(ApicurioConfig, type)

        # Test exception classes
        assert issubclass(AvroCurioError, Exception)
        assert issubclass(SchemaNotFoundError, AvroCurioError)
        assert issubclass(InvalidWireFormatError, AvroCurioError)
        assert issubclass(SerializationError, AvroCurioError)
        assert issubclass(DeserializationError, AvroCurioError)
        assert issubclass(SchemaMismatchError, AvroCurioError)


class TestCreateSerializer:
    """Test the create_serializer convenience function."""

    @pytest.mark.asyncio
    async def test_create_serializer_basic(self):
        """Test basic serializer creation."""
        config = ApicurioConfig(base_url="http://test:8080")

        serializer = await create_serializer(config)

        assert isinstance(serializer, AvroSerializer)
        assert isinstance(serializer.client, ApicurioClient)
        assert serializer.client.config == config

    @pytest.mark.asyncio
    async def test_create_serializer_with_auth(self):
        """Test serializer creation with authentication."""
        config = ApicurioConfig(base_url="http://test:8080", auth=("user", "pass"))

        serializer = await create_serializer(config)

        assert isinstance(serializer, AvroSerializer)
        assert serializer.client.config.auth == ("user", "pass")

    @pytest.mark.asyncio
    async def test_create_serializer_returns_configured_client(self):
        """Test that serializer is properly configured."""
        config = ApicurioConfig()

        serializer = await create_serializer(config)

        assert isinstance(serializer, AvroSerializer)
        assert isinstance(serializer.client, ApicurioClient)
