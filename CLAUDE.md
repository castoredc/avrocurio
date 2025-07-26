# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AvroCurio is a Python library that provides Apache Avro serialization/deserialization with Confluent Schema Registry wire format and Apicurio Schema Registry integration. It enables async serialization of dataclasses-avroschema models with automatic schema management and caching.

## Development Commands

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_serializer.py

# Run with verbose output
uv run pytest -v

# Skip integration tests (these require Apicurio Registry)
uv run pytest -m "not integration"
```

### Code Quality
```bash
# Lint code with Ruff
uv run ruff check

# Format code with Ruff
uv run ruff format

# Fix auto-fixable linting issues
uv run ruff check --fix
```

### Development Environment
```bash
# Start Apicurio Registry for integration tests
docker compose up

# Install development dependencies
uv sync --dev
```

## Architecture

### Core Components

- **ApicurioClient** (`src/avrocurio/schema_client.py`): Async HTTP client for Apicurio Schema Registry API with TTL caching for schemas and error responses
- **AvroSerializer** (`src/avrocurio/serializer.py`): Main serialization/deserialization class using Confluent wire format
- **ConfluentWireFormat** (`src/avrocurio/wire_format.py`): Handles the binary wire format (magic byte + 4-byte schema ID + Avro payload)
- **ApicurioConfig** (`src/avrocurio/config.py`): Configuration dataclass for registry connection settings

### Wire Format Implementation

The library implements the Confluent Schema Registry wire format:
- 1 byte magic byte (0x0)
- 4 bytes big-endian schema ID
- Remaining bytes: Avro binary payload

### Schema Management

- Automatic schema lookup and caching in ApicurioClient
- Support for both automatic schema discovery and explicit schema ID specification
- Error caching to avoid repeated failed lookups
- Integration with dataclasses-avroschema for Python dataclass models

### Testing Structure

- Unit tests for core functionality
- Integration tests marked with `@pytest.mark.integration` that require a running Apicurio Registry
- Mock-based testing for HTTP interactions
- Async test support with pytest-asyncio

## Key Dependencies

- `fastavro`: Avro serialization/deserialization
- `httpx`: Async HTTP client for registry communication
- `dataclasses-avroschema`: Python dataclass to Avro schema conversion
- `cachetools`: TTL caching for schema and error responses
