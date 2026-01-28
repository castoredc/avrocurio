"""In-memory implementation of schema registry client for testing and development."""

import asyncio
import json
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

from .exceptions import AvroCurioError, SchemaNotFoundError


@dataclass
class SchemaVersion:
    """Represents a version of a schema artifact."""

    version: int
    global_id: int
    schema_content: str


class InMemoryClient:
    """
    In-memory implementation of schema registry client.

    This client provides the same async interface as ApicurioClient but stores
    all data in memory without any external dependencies. It's useful for:

    - Testing without Docker or Apicurio Registry
    - Local development and prototyping
    - CI/CD pipelines without infrastructure setup
    - Fast deterministic tests
    - Demos and examples

    Unlike ApicurioClient, this implementation requires no configuration and
    has no caching layer (since everything is already in-memory).
    """

    def __init__(self) -> None:
        """Initialize in-memory client with empty storage."""
        self._schemas: dict[int, dict[str, Any]] = {}  # global_id -> schema_dict
        self._artifacts: dict[tuple[str, str], list[SchemaVersion]] = {}  # (group_id, artifact_id) -> versions
        self._schema_to_global_id: dict[str, int] = {}  # normalized_schema -> global_id
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.close()

    async def close(self) -> None:
        """Close the client (no-op)."""

    def _normalize_schema(self, schema_content: str) -> str:
        """
        Normalize schema to canonical JSON form for comparison.

        Args:
            schema_content: Schema content as JSON string

        Returns:
            Normalized schema string

        """
        schema_dict = json.loads(schema_content)
        return json.dumps(schema_dict, sort_keys=True, separators=(",", ":"))

    def _get_next_global_id(self) -> int:
        """
        Get the next available global ID.

        Returns:
            Next global ID (max existing ID + 1, or 1 if no schemas exist)

        """
        if not self._schemas:
            return 1
        return max(self._schemas.keys()) + 1

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
        async with self._lock:
            if global_id not in self._schemas:
                msg = f"Schema with global ID {global_id} not found"
                raise SchemaNotFoundError(msg)
            return self._schemas[global_id]

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
        async with self._lock:
            key = (group_id, artifact_id)
            if key not in self._artifacts or not self._artifacts[key]:
                msg = f"Schema {group_id}/{artifact_id} not found"
                raise SchemaNotFoundError(msg)

            latest_version = self._artifacts[key][-1]
            if latest_version.global_id not in self._schemas:
                msg = f"Schema with global ID {latest_version.global_id} not found"
                raise SchemaNotFoundError(msg)
            schema = self._schemas[latest_version.global_id]
            return latest_version.global_id, schema

    async def register_schema(
        self,
        group: str,
        artifact_name: str,
        schema_content: str,
        artifact_type: str = "AVRO",  # noqa: ARG002
    ) -> int:
        """
        Register a new schema in the registry.

        This method is idempotent - registering the same schema content
        for the same artifact will return the same global ID. However,
        different artifacts can have the same schema content (each gets
        a version pointing to the same schema).

        Args:
            group: Group for the schema
            artifact_name: Artifact name for the schema
            schema_content: Schema content as JSON string
            artifact_type: Type of artifact (default: "AVRO")

        Returns:
            Global ID of the registered schema

        """
        # Normalize schema for comparison
        normalized = self._normalize_schema(schema_content)

        async with self._lock:
            # Check if this exact schema already exists (for idempotency)
            if normalized in self._schema_to_global_id:
                existing_id = self._schema_to_global_id[normalized]
                # Check if this artifact already has this schema
                key = (group, artifact_name)
                if key in self._artifacts:
                    for version in self._artifacts[key]:
                        if version.global_id == existing_id:
                            return existing_id
                # Schema exists but not for this artifact - create new version
                if key not in self._artifacts:
                    self._artifacts[key] = []
                version_num = len(self._artifacts[key]) + 1
                schema_version = SchemaVersion(
                    version=version_num,
                    global_id=existing_id,
                    schema_content=schema_content,
                )
                self._artifacts[key].append(schema_version)
                return existing_id

            # Create new global ID
            global_id = self._get_next_global_id()

            # Parse and store schema
            schema_dict = json.loads(schema_content)
            self._schemas[global_id] = schema_dict
            self._schema_to_global_id[normalized] = global_id

            # Create or update artifact versions
            key = (group, artifact_name)
            if key not in self._artifacts:
                self._artifacts[key] = []

            version_num = len(self._artifacts[key]) + 1
            schema_version = SchemaVersion(
                version=version_num,
                global_id=global_id,
                schema_content=schema_content,
            )
            self._artifacts[key].append(schema_version)

            return global_id

    async def register_schema_version(self, group_id: str, artifact_id: str, schema_content: str) -> int:
        """
        Register a new version of an existing schema.

        Args:
            group_id: Group ID for the schema
            artifact_id: Artifact ID for the schema
            schema_content: Schema content as JSON string

        Returns:
            Global ID of the registered schema version

        Raises:
            AvroCurioError: If artifact does not exist

        """
        # Normalize schema
        normalized = self._normalize_schema(schema_content)

        async with self._lock:
            key = (group_id, artifact_id)
            if key not in self._artifacts:
                msg = f"Artifact {group_id}/{artifact_id} does not exist"
                raise AvroCurioError(msg)

            # Check if this exact schema already exists
            if normalized in self._schema_to_global_id:
                existing_id = self._schema_to_global_id[normalized]
                # Check if it's already a version of this artifact
                for version in self._artifacts[key]:
                    if version.global_id == existing_id:
                        return existing_id
                # It exists but not for this artifact, add new version pointing to same schema
                version_num = len(self._artifacts[key]) + 1
                schema_version = SchemaVersion(
                    version=version_num,
                    global_id=existing_id,
                    schema_content=schema_content,
                )
                self._artifacts[key].append(schema_version)
                return existing_id

            # Create new global ID for new schema content
            global_id = self._get_next_global_id()

            # Parse and store schema
            schema_dict = json.loads(schema_content)
            self._schemas[global_id] = schema_dict
            self._schema_to_global_id[normalized] = global_id

            # Add new version
            version_num = len(self._artifacts[key]) + 1
            schema_version = SchemaVersion(
                version=version_num,
                global_id=global_id,
                schema_content=schema_content,
            )
            self._artifacts[key].append(schema_version)

            return global_id

    async def search_artifacts(self, name: str | None = None, artifact_type: str = "AVRO") -> list[dict[str, Any]]:
        """
        Search for artifacts in the registry.

        Args:
            name: Optional name filter for artifacts
            artifact_type: Type of artifacts to search for (default: "AVRO")

        Returns:
            List of artifact metadata dictionaries

        """
        async with self._lock:
            results = []
            for (group_id, artifact_id), versions in self._artifacts.items():
                # Filter by name if provided (case-insensitive substring match)
                if name and name.lower() not in artifact_id.lower():
                    continue

                # Note: We don't track artifact_type in this simple implementation,
                # so we assume all artifacts match the type filter
                if versions:
                    latest_version = versions[-1]
                    results.append(
                        {
                            "groupId": group_id,
                            "artifactId": artifact_id,
                            "id": artifact_id,
                            "name": artifact_id,
                            "artifactType": artifact_type,
                            "globalId": latest_version.global_id,
                            "version": latest_version.version,
                        }
                    )

            return results

    async def check_artifact_exists(self, group_id: str, artifact_id: str) -> bool:
        """
        Check if an artifact exists in the registry.

        Args:
            group_id: Group ID for the artifact
            artifact_id: Artifact ID to check

        Returns:
            True if artifact exists, False otherwise

        """
        async with self._lock:
            key = (group_id, artifact_id)
            return key in self._artifacts and len(self._artifacts[key]) > 0

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

        """
        # Normalize the search schema
        normalized = self._normalize_schema(schema_content)

        async with self._lock:
            # Search through all artifacts
            for (art_group_id, artifact_id), versions in self._artifacts.items():
                # Filter by group if specified
                if group_id and art_group_id != group_id:
                    continue

                # Check if any version matches
                for version in versions:
                    version_normalized = self._normalize_schema(version.schema_content)
                    if version_normalized == normalized:
                        return art_group_id, artifact_id

            return None

    async def clear_cache(self) -> None:
        """
        Clear cache (no-op for in-memory implementation).

        Kept for API compatibility with ApicurioClient.
        """

    async def get_cache_stats(self) -> dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache size information

        Note:
            Since this is an in-memory implementation with no caching layer,
            this returns hardcoded values for API compatibility.

        """
        return {
            "schema_cache_size": len(self._schemas),
            "schema_cache_max_size": 0,
            "failed_lookup_cache_size": 0,
            "failed_lookup_cache_max_size": 0,
            "failed_lookup_cache_ttl": 0,
        }

    # Helper methods for testing

    async def add_schema(self, global_id: int, schema_dict: dict[str, Any], group_id: str, artifact_id: str) -> None:
        """
        Pre-populate a schema in the registry (helper for tests).

        Args:
            global_id: Global ID to assign
            schema_dict: Schema as a dictionary
            group_id: Group ID for the artifact
            artifact_id: Artifact ID for the schema

        """
        async with self._lock:
            # Store schema
            self._schemas[global_id] = schema_dict

            # Store normalized mapping
            schema_content = json.dumps(schema_dict)
            normalized = self._normalize_schema(schema_content)
            self._schema_to_global_id[normalized] = global_id

            # Add to artifacts
            key = (group_id, artifact_id)
            if key not in self._artifacts:
                self._artifacts[key] = []

            version_num = len(self._artifacts[key]) + 1
            schema_version = SchemaVersion(
                version=version_num,
                global_id=global_id,
                schema_content=schema_content,
            )
            self._artifacts[key].append(schema_version)

    async def reset(self) -> None:
        """
        Clear all internal state (helper for tests).

        This resets the client to a fresh state, removing all registered
        schemas and artifacts.
        """
        async with self._lock:
            self._schemas.clear()
            self._artifacts.clear()
            self._schema_to_global_id.clear()
