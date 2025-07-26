# AvroCurio

Apache Avro serialization/deserialization with Confluent Schema Registry framing using Apicurio Schema Registry.

## Installation

Using `uv`, `poetry`, or `pip`:

```bash
uv add avrocurio
```

```bash
poetry add avrocurio
```

```bash
pip install avrocurio
```

## Quick Start

### 1. Define your schema using dataclasses-avroschema

```python
from dataclasses import dataclass
from dataclasses_avroschema import AvroModel

@dataclass
class User(AvroModel):
    name: str
    age: int
    email: str
```

### 2. Serialize and deserialize data

```python
import asyncio
from avrocurio import AvroSerializer, ApicurioClient, ApicurioConfig

async def main():
    # Configure connection to Apicurio Registry
    config = ApicurioConfig(base_url="http://localhost:8080")

    # Create client and serializer
    async with ApicurioClient(config) as client:
        serializer = AvroSerializer(client)

        # Create a user instance
        user = User(name="John Doe", age=30, email="john@example.com")

        # Serialize the user to an Avro binary with Confluent registry framing.
        # Under the hood this will perform a lookup against Apicurio to get the
        # artifact ID for the schema, which is then prepended to the Avro binary
        # (along with a magic byte).
        serialized = await serializer.serialize(user)

        # Deserialize the binary back to a User instance.
        deserialized_user = await serializer.deserialize(serialized, User)

asyncio.run(main())
```

## Confluent Schema Registry Wire Format

AvroCurio implements the Confluent Schema Registry wire format:

```
+----------------+------------------+------------------+
| Magic Byte     | Schema ID        | Avro Payload     |
| (1 byte = 0x0) | (4 bytes, BE)    | (remaining)      |
+----------------+------------------+------------------+
```

- **Magic Byte**: Always `0x0` to identify Confluent wire format
- **Schema ID**: 4-byte big-endian integer referencing the schema in the registry
- **Avro Payload**: Standard Avro binary-encoded data

## Error Handling

AvroCurio provides specific exception types:

- `AvroCurioError` - Base exception for all library errors
- `SchemaNotFoundError` - Schema not found in registry
- `InvalidWireFormatError` - Message doesn't follow Confluent wire format
- `SerializationError` - Failed to serialize data
- `DeserializationError` - Failed to deserialize data
- `SchemaMismatchError` - Data doesn't match expected schema
- `SchemaMatchError` - No matching schema found during automatic lookup

### Schema Caching

Schema caching is enabled by default for performance. Disable if needed:

```python
serializer = AvroSerializer(client, cache_schemas=False)
```

### Getting Schema Information

```python
# Get schema with metadata
obj, schema = await serializer.deserialize_with_schema(message, User)
print(f"Schema: {schema}")

# Search for schemas
artifacts = await client.search_artifacts(name="user", artifact_type="AVRO")
```

## Development

### Requirements

Integration tests require a running Apicurio Registry.
Running it through Docker is the easiest way:

```bash
docker run -p 8080:8080 quay.io/apicurio/apicurio-registry-mem:latest
```

Port 8080 is assumed by default, but you can set `APICURIO_URL` to point to a different instance.

### Running Tests

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_serializer.py

# Run with verbose output
uv run pytest -v

# Skip integration tests
uv run pytest -m "not integration"
```
