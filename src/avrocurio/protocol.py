"""Protocol definitions for AvroCurio."""

from typing import Any, Protocol


class SchemaRegistryClient(Protocol):
    """
    Protocol for schema registry clients.

    This protocol defines the interface that schema registry clients must implement
    to be compatible with AvroSerializer. Both ApicurioClient and InMemoryClient
    implement this protocol.

    The protocol ensures type safety when passing different client implementations
    to AvroSerializer while maintaining flexibility for testing and production use.
    """

    async def get_schema_by_global_id(self, global_id: int) -> dict[str, Any]:
        """
        Fetch schema by its global ID.

        Args:
            global_id: Global ID of the schema

        Returns:
            Schema as a dictionary

        Raises:
            SchemaNotFoundError: If schema with given ID is not found

        """
        ...

    async def get_latest_schema(self, group_id: str, artifact_id: str) -> tuple[int, dict[str, Any]]:
        """
        Get the latest version of a schema by group and artifact ID.

        Args:
            group_id: Group ID containing the artifact
            artifact_id: Artifact ID of the schema

        Returns:
            Tuple of (global_id, schema)

        Raises:
            SchemaNotFoundError: If schema is not found

        """
        ...

    async def find_artifact_by_content(
        self, schema_content: str, group_id: str | None = None
    ) -> tuple[str, str] | None:
        """
        Find artifact by searching for matching schema content.

        Args:
            schema_content: Schema content as JSON string
            group_id: Optional group ID to filter search

        Returns:
            Tuple of (group_id, artifact_id) if found, None otherwise

        Raises:
            AvroCurioError: For API errors

        """
        ...

    async def register_schema(
        self, group: str, artifact_name: str, schema_content: str, artifact_type: str = "AVRO"
    ) -> int:
        """
        Register a new schema in the registry.

        Args:
            group: Group for the schema
            artifact_name: Artifact name for the schema
            schema_content: Schema content as JSON string
            artifact_type: Type of artifact (default: "AVRO")

        Returns:
            Global ID of the registered schema

        Raises:
            AvroCurioError: For API errors

        """
        ...
