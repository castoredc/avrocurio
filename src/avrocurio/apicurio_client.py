"""Async client for Apicurio Schema Registry."""

import asyncio
import json
from dataclasses import dataclass
from http import HTTPStatus
from types import TracebackType
from typing import Any

import httpx
from cachetools import TTLCache

from .config import ApicurioConfig
from .exceptions import AvroCurioError, SchemaNotFoundError


@dataclass
class CachedError:
    """Cached error details for failed schema lookups."""

    global_id: int
    status_code: int
    timestamp: float


class ApicurioClient:
    """
    Async client for interacting with Apicurio Schema Registry.

    This client provides methods to fetch schemas and manage schema registry operations
    using the Apicurio Registry v3 API with LRU caching for performance.
    """

    def __init__(self, config: ApicurioConfig) -> None:
        """
        Initialize the Apicurio client.

        Args:
            config: Configuration for the Apicurio Registry connection

        """
        self.config = config
        auth = None
        if self.config.auth:
            auth = httpx.BasicAuth(self.config.auth[0], self.config.auth[1])

        self._client = httpx.AsyncClient(
            base_url=config.base_url,
            timeout=config.timeout,
            auth=auth,
            headers={"Content-Type": "application/json"},
        )

        self._schema_cache = TTLCache(
            maxsize=config.schema_cache_size,
            ttl=float("inf"),
        )
        self._schema_cache_lock = asyncio.Lock()

        self._failed_lookup_cache = TTLCache(
            maxsize=config.failed_lookup_cache_size,
            ttl=config.failed_lookup_cache_ttl,
        )
        self._failed_lookup_cache_lock = asyncio.Lock()

    async def __aenter__(self) -> "ApicurioClient":
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
        """Close the HTTP client and clear caches."""
        await self._client.aclose()
        self._schema_cache.clear()
        self._failed_lookup_cache.clear()

    async def _raise_if_previous_lookup_failed(self, global_id: int) -> None:
        """Check failed lookup cache and raise if found."""
        async with self._failed_lookup_cache_lock:
            if global_id in self._failed_lookup_cache:
                error_details = self._failed_lookup_cache[global_id]
                current_time = asyncio.get_event_loop().time()
                msg = (
                    f"Schema with global ID {global_id} not found "
                    f"(cached {round(current_time - error_details.timestamp, 1)} seconds ago)"
                )
                raise SchemaNotFoundError(msg)

    async def _cache_failed_lookup(self, global_id: int) -> None:
        """Cache a failed schema lookup."""
        error_details = CachedError(
            global_id=global_id,
            status_code=HTTPStatus.NOT_FOUND,
            timestamp=asyncio.get_event_loop().time(),
        )
        async with self._failed_lookup_cache_lock:
            self._failed_lookup_cache[global_id] = error_details

    async def get_schema_by_global_id(self, global_id: int) -> dict[str, Any]:
        """
        Fetch schema by its global ID with LRU caching.

        Args:
            global_id: Global ID of the schema in Apicurio Registry

        Returns:
            Schema as a dictionary

        Raises:
            SchemaNotFoundError: If schema with given ID is not found
            AvroCurioError: For other API errors

        """
        async with self._schema_cache_lock:
            cached_schema = self._schema_cache.get(global_id)
            if cached_schema is not None:
                return cached_schema
        await self._raise_if_previous_lookup_failed(global_id)

        try:
            url = f"/apis/registry/v3/ids/globalIds/{global_id}"
            response = await self._client.get(url)
            response.raise_for_status()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == HTTPStatus.NOT_FOUND:
                await self._cache_failed_lookup(global_id)
                msg = f"Schema with global ID {global_id} not found"
                raise SchemaNotFoundError(msg) from e

            msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            raise AvroCurioError(msg) from e
        except httpx.RequestError as e:
            msg = f"Request error: {e}"
            raise AvroCurioError(msg) from e

        try:
            schema = json.loads(response.text)
        except json.JSONDecodeError as e:
            msg = f"Invalid JSON schema content: {e}"
            raise AvroCurioError(msg) from e

        async with self._schema_cache_lock:
            self._schema_cache[global_id] = schema
        return schema

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
            AvroCurioError: For other API errors

        """
        try:
            # First get the artifact metadata to find the latest version
            url = f"/apis/registry/v3/groups/{group_id}/artifacts/{artifact_id}/versions/branch=latest"
            response = await self._client.get(url)

            if response.status_code == HTTPStatus.NOT_FOUND:
                msg = f"Schema {group_id}/{artifact_id} not found"
                raise SchemaNotFoundError(msg)

            response.raise_for_status()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == HTTPStatus.NOT_FOUND:
                msg = f"Schema {group_id}/{artifact_id} not found"
                raise SchemaNotFoundError(msg) from e
            msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            raise AvroCurioError(msg) from e
        except httpx.RequestError as e:
            msg = f"Request error: {e}"
            raise AvroCurioError(msg) from e

        version_metadata = response.json()
        global_id = version_metadata.get("globalId")
        if global_id is None:
            msg = f"No global ID found in metadata for {group_id}/{artifact_id}"
            raise AvroCurioError(msg)

        # Now fetch the actual schema content
        schema = await self.get_schema_by_global_id(global_id)
        return global_id, schema

    async def search_artifacts(self, name: str | None = None, artifact_type: str = "AVRO") -> list[dict[str, Any]]:
        """
        Search for artifacts in the registry.

        Args:
            name: Optional name filter for artifacts
            artifact_type: Type of artifacts to search for (default: "AVRO")

        Returns:
            List of artifact metadata dictionaries

        Raises:
            AvroCurioError: For API errors

        """
        try:
            url = "/apis/registry/v3/search/artifacts"
            params = {"artifactType": artifact_type}
            if name:
                params["name"] = name

            response = await self._client.get(url, params=params)
            response.raise_for_status()

            search_results = response.json()
            return search_results.get("artifacts", [])

        except httpx.HTTPStatusError as e:
            msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            raise AvroCurioError(msg) from e
        except httpx.RequestError as e:
            msg = f"Request error: {e}"
            raise AvroCurioError(msg) from e

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
        try:
            url = f"/apis/registry/v3/groups/{group}/artifacts"

            artifact_data = {
                "artifactId": artifact_name,
                "artifactType": artifact_type,
                "firstVersion": {"content": {"content": schema_content, "contentType": "application/json"}},
            }

            # FIND_OR_CREATE_VERSION ensures idempotent behavior here
            params = {"ifExists": "FIND_OR_CREATE_VERSION"}
            response = await self._client.post(
                url, json=artifact_data, params=params, headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()

        except httpx.HTTPStatusError as e:
            msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            raise AvroCurioError(msg) from e
        except httpx.RequestError as e:
            msg = f"Request error: {e}"
            raise AvroCurioError(msg) from e

        # Get global ID from response (v3 API returns nested structure)
        response_data = response.json()
        version_metadata = response_data.get("version", {})
        global_id = version_metadata.get("globalId")
        if global_id is None:
            msg = f"No global ID returned for registered schema {group}/{artifact_name}"
            raise AvroCurioError(msg)

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
            AvroCurioError: For API errors

        """
        try:
            url = f"/apis/registry/v3/groups/{group_id}/artifacts/{artifact_id}/versions"

            # Create version request body using v3 API format
            version_data = {"content": {"content": schema_content, "contentType": "application/json"}}

            response = await self._client.post(url, json=version_data, headers={"Content-Type": "application/json"})
            response.raise_for_status()

        except httpx.HTTPStatusError as e:
            msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            raise AvroCurioError(msg) from e
        except httpx.RequestError as e:
            msg = f"Request error: {e}"
            raise AvroCurioError(msg) from e

        # Get global ID from response
        version_metadata = response.json()
        global_id = version_metadata.get("globalId")
        if global_id is None:
            msg = f"No global ID returned for schema version {group_id}/{artifact_id}"
            raise AvroCurioError(msg)

        return global_id

    async def check_artifact_exists(self, group_id: str, artifact_id: str) -> bool:
        """
        Check if an artifact exists in the registry.

        Args:
            group_id: Group ID for the artifact
            artifact_id: Artifact ID to check

        Returns:
            True if artifact exists, False otherwise

        """
        try:
            url = f"/apis/registry/v3/groups/{group_id}/artifacts/{artifact_id}"
            response = await self._client.get(url)
        except httpx.HTTPStatusError:
            return False
        except httpx.RequestError:
            return False
        else:
            return response.status_code == HTTPStatus.OK

    async def find_artifact_by_content(
        self, schema_content: str, group_id: str | None = None
    ) -> tuple[str, str] | None:
        """
        Find artifact by searching registry with schema content.

        Uses Apicurio's canonical content search to find matching schemas.

        Args:
            schema_content: Schema content as JSON string
            group_id: Optional group ID to filter search

        Returns:
            Tuple of (group_id, artifact_id) if found, None otherwise

        Raises:
            AvroCurioError: For API errors

        """
        try:
            url = "/apis/registry/v3/search/artifacts"
            params = {
                "canonical": "true",
                "artifactType": "AVRO",
                "limit": 10,  # Limit results for performance
            }
            if group_id:
                params["groupId"] = group_id

            response = await self._client.post(
                url, content=schema_content, params=params, headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()

        except httpx.HTTPStatusError as e:
            msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            raise AvroCurioError(msg) from e
        except httpx.RequestError as e:
            msg = f"Request error: {e}"
            raise AvroCurioError(msg) from e

        search_results = response.json()
        artifacts = search_results.get("artifacts", [])
        if artifacts:
            first_artifact = artifacts[0]
            artifact_id = (
                first_artifact.get("artifactId") or first_artifact.get("id") or first_artifact.get("name", "unknown")
            )
            group_id_result = first_artifact.get("groupId", "default")
            return group_id_result, artifact_id

        return None

    async def clear_cache(self) -> None:
        """Clear all cached schemas and failed lookups."""
        async with self._schema_cache_lock:
            self._schema_cache.clear()
        async with self._failed_lookup_cache_lock:
            self._failed_lookup_cache.clear()

    async def get_cache_stats(self) -> dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache size information

        """
        async with self._schema_cache_lock:
            schema_size = len(self._schema_cache)
            schema_max_size = self._schema_cache.maxsize

        async with self._failed_lookup_cache_lock:
            failed_size = len(self._failed_lookup_cache)
            failed_max_size = self._failed_lookup_cache.maxsize
            failed_ttl = self._failed_lookup_cache.ttl

        return {
            "schema_cache_size": schema_size,
            "schema_cache_max_size": schema_max_size,
            "failed_lookup_cache_size": failed_size,
            "failed_lookup_cache_max_size": failed_max_size,
            "failed_lookup_cache_ttl": failed_ttl,
        }
