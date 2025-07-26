"""Async client for Apicurio Schema Registry."""

import json
from typing import Any, Optional
import httpx
import fastavro
from fastavro.schema import to_parsing_canonical_form, fingerprint
from .config import ApicurioConfig
from .exceptions import SchemaNotFoundError, AvroCurioError


class ApicurioClient:
    """Async client for interacting with Apicurio Schema Registry.
    
    This client provides methods to fetch schemas and manage schema registry operations
    using the Apicurio Registry v3 API.
    """
    
    def __init__(self, config: ApicurioConfig):
        """Initialize the Apicurio client.
        
        Args:
            config: Configuration for the Apicurio Registry connection
        """
        self.config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._schema_cache: dict[int, dict[str, Any]] = {}
    
    async def __aenter__(self) -> "ApicurioClient":
        """Async context manager entry."""
        await self._ensure_client()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
    
    async def _ensure_client(self) -> None:
        """Ensure HTTP client is initialized."""
        if self._client is None:
            auth = None
            if self.config.auth:
                auth = httpx.BasicAuth(self.config.auth[0], self.config.auth[1])
            
            self._client = httpx.AsyncClient(
                base_url=self.config.base_url,
                timeout=self.config.timeout,
                auth=auth,
                headers={"Content-Type": "application/json"}
            )
    
    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
    
    async def get_schema_by_global_id(self, global_id: int) -> dict[str, Any]:
        """Fetch schema by its global ID.
        
        Args:
            global_id: Global ID of the schema in Apicurio Registry
            
        Returns:
            Schema as a dictionary
            
        Raises:
            SchemaNotFoundError: If schema with given ID is not found
            AvroCurioError: For other API errors
        """
        # Check cache first
        if global_id in self._schema_cache:
            return self._schema_cache[global_id]
        
        await self._ensure_client()
        
        try:
            url = f"/apis/registry/v3/ids/globalIds/{global_id}"
            response = await self._client.get(url)
            
            if response.status_code == 404:
                raise SchemaNotFoundError(f"Schema with global ID {global_id} not found")
            
            response.raise_for_status()
            
            # Parse the schema content
            schema_content = response.text
            try:
                schema = json.loads(schema_content)
            except json.JSONDecodeError:
                # If it's not valid JSON, treat it as raw schema content
                schema = {"schema": schema_content}
            
            # Cache the schema
            self._schema_cache[global_id] = schema
            return schema
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise SchemaNotFoundError(f"Schema with global ID {global_id} not found")
            raise AvroCurioError(f"HTTP error {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise AvroCurioError(f"Request error: {e}")
    
    async def get_latest_schema(self, group_id: str, artifact_id: str) -> tuple[int, dict[str, Any]]:
        """Get the latest version of a schema by group and artifact ID.
        
        Args:
            group_id: Group ID containing the artifact
            artifact_id: Artifact ID of the schema
            
        Returns:
            Tuple of (global_id, schema)
            
        Raises:
            SchemaNotFoundError: If schema is not found
            AvroCurioError: For other API errors
        """
        await self._ensure_client()
        
        try:
            # First get the artifact metadata to find the latest version
            url = f"/apis/registry/v3/groups/{group_id}/artifacts/{artifact_id}/versions/branch=latest"
            response = await self._client.get(url)
            
            if response.status_code == 404:
                raise SchemaNotFoundError(f"Schema {group_id}/{artifact_id} not found")
            
            response.raise_for_status()
            version_metadata = response.json()
            
            # Extract global ID from metadata
            global_id = version_metadata.get("globalId")
            if global_id is None:
                raise AvroCurioError(f"No global ID found in metadata for {group_id}/{artifact_id}")
            
            # Now fetch the actual schema content
            schema = await self.get_schema_by_global_id(global_id)
            return global_id, schema
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise SchemaNotFoundError(f"Schema {group_id}/{artifact_id} not found")
            raise AvroCurioError(f"HTTP error {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise AvroCurioError(f"Request error: {e}")
    
    async def search_artifacts(
        self, 
        name: Optional[str] = None, 
        artifact_type: str = "AVRO"
    ) -> list[dict[str, Any]]:
        """Search for artifacts in the registry.
        
        Args:
            name: Optional name filter for artifacts
            artifact_type: Type of artifacts to search for (default: "AVRO")
            
        Returns:
            List of artifact metadata dictionaries
            
        Raises:
            AvroCurioError: For API errors
        """
        await self._ensure_client()
        
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
            raise AvroCurioError(f"HTTP error {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise AvroCurioError(f"Request error: {e}")
    
    async def register_schema(
        self, 
        group_id: str, 
        artifact_id: str, 
        schema_content: str,
        artifact_type: str = "AVRO"
    ) -> int:
        """Register a new schema in the registry.
        
        Args:
            group_id: Group ID for the schema
            artifact_id: Artifact ID for the schema
            schema_content: Schema content as JSON string
            artifact_type: Type of artifact (default: "AVRO")
            
        Returns:
            Global ID of the registered schema
            
        Raises:
            AvroCurioError: For API errors
        """
        await self._ensure_client()
        
        try:
            url = f"/apis/registry/v3/groups/{group_id}/artifacts"
            
            # Create artifact request body using v3 API format
            artifact_data = {
                "artifactId": artifact_id,
                "artifactType": artifact_type,
                "firstVersion": {
                    "content": {
                        "content": schema_content,
                        "contentType": "application/json"
                    }
                }
            }
            
            # Use CREATE_VERSION to handle existing artifacts gracefully
            params = {"ifExists": "CREATE_VERSION"}
            
            response = await self._client.post(
                url,
                json=artifact_data,
                params=params,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 409:
                # Artifact already exists, try to create a new version
                return await self.register_schema_version(group_id, artifact_id, schema_content)
            
            response.raise_for_status()
            
            # Get global ID from response (v3 API returns nested structure)
            response_data = response.json()
            version_metadata = response_data.get("version", {})
            global_id = version_metadata.get("globalId")
            if global_id is None:
                raise AvroCurioError(f"No global ID returned for registered schema {group_id}/{artifact_id}")
            
            return global_id
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                # Try to create new version instead
                return await self.register_schema_version(group_id, artifact_id, schema_content)
            raise AvroCurioError(f"HTTP error {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise AvroCurioError(f"Request error: {e}")
    
    async def register_schema_version(
        self, 
        group_id: str, 
        artifact_id: str, 
        schema_content: str
    ) -> int:
        """Register a new version of an existing schema.
        
        Args:
            group_id: Group ID for the schema
            artifact_id: Artifact ID for the schema
            schema_content: Schema content as JSON string
            
        Returns:
            Global ID of the registered schema version
            
        Raises:
            AvroCurioError: For API errors
        """
        await self._ensure_client()
        
        try:
            url = f"/apis/registry/v3/groups/{group_id}/artifacts/{artifact_id}/versions"
            
            # Create version request body using v3 API format
            version_data = {
                "content": {
                    "content": schema_content,
                    "contentType": "application/json"
                }
            }
            
            response = await self._client.post(
                url,
                json=version_data,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
            # Get global ID from response
            version_metadata = response.json()
            global_id = version_metadata.get("globalId")
            if global_id is None:
                raise AvroCurioError(f"No global ID returned for schema version {group_id}/{artifact_id}")
            
            return global_id
            
        except httpx.HTTPStatusError as e:
            raise AvroCurioError(f"HTTP error {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise AvroCurioError(f"Request error: {e}")
    
    async def check_artifact_exists(self, group_id: str, artifact_id: str) -> bool:
        """Check if an artifact exists in the registry.
        
        Args:
            group_id: Group ID for the artifact
            artifact_id: Artifact ID to check
            
        Returns:
            True if artifact exists, False otherwise
        """
        await self._ensure_client()
        
        try:
            url = f"/apis/registry/v3/groups/{group_id}/artifacts/{artifact_id}"
            response = await self._client.get(url)
            return response.status_code == 200
        except httpx.HTTPStatusError:
            return False
        except httpx.RequestError:
            return False
    
    async def find_artifact_by_content(
        self, 
        schema_content: str,
        group_id: Optional[str] = None
    ) -> Optional[tuple[str, str]]:
        """Find artifact by searching registry with schema content.
        
        Uses Apicurio's canonical content search to find matching schemas.
        
        Args:
            schema_content: Schema content as JSON string
            group_id: Optional group ID to filter search
            
        Returns:
            Tuple of (group_id, artifact_id) if found, None otherwise
            
        Raises:
            AvroCurioError: For API errors
        """
        await self._ensure_client()
        
        try:
            url = "/apis/registry/v3/search/artifacts"
            params = {
                "canonical": "true",
                "artifactType": "AVRO",
                "limit": 10  # Limit results for performance
            }
            if group_id:
                params["groupId"] = group_id
            
            response = await self._client.post(
                url,
                content=schema_content,
                params=params,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
            search_results = response.json()
            artifacts = search_results.get("artifacts", [])
            
            if artifacts:
                # Return the first match - handle different API response structures
                first_artifact = artifacts[0]
                
                # Extract artifact ID
                artifact_id = (
                    first_artifact.get("artifactId") or 
                    first_artifact.get("id") or 
                    first_artifact.get("name", "unknown")
                )
                
                # Extract group ID (defaults to "default" if not present)
                group_id = first_artifact.get("groupId", "default")
                
                return group_id, artifact_id
            
            return None
            
        except httpx.HTTPStatusError as e:
            raise AvroCurioError(f"HTTP error {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise AvroCurioError(f"Request error: {e}")
    
    async def find_artifact_by_fingerprint(
        self, 
        schema_content: str,
        group_id: Optional[str] = None
    ) -> Optional[tuple[str, str]]:
        """Find artifact by comparing schema fingerprints.
        
        Uses fastavro fingerprinting as fallback when content search fails.
        
        Args:
            schema_content: Schema content as JSON string
            group_id: Optional group ID to filter search
            
        Returns:
            Tuple of (group_id, artifact_id) if found, None otherwise
            
        Raises:
            AvroCurioError: For API errors
        """
        await self._ensure_client()
        
        try:
            # Generate fingerprint for target schema
            target_schema = json.loads(schema_content)
            target_canonical = to_parsing_canonical_form(target_schema)
            target_fingerprint = fingerprint(target_canonical, algorithm='md5')
            
            # Search for all artifacts to compare fingerprints
            search_params = {"artifactType": "AVRO", "limit": 100}
            if group_id:
                search_params["groupId"] = group_id
            
            url = "/apis/registry/v3/search/artifacts"
            response = await self._client.get(url, params=search_params)
            response.raise_for_status()
            
            search_results = response.json()
            artifacts = search_results.get("artifacts", [])
            
            # Check each artifact's latest schema fingerprint
            for artifact in artifacts:
                try:
                    # Get latest schema for this artifact
                    artifact_group_id = artifact["groupId"]
                    artifact_id = artifact["artifactId"]
                    
                    _, schema = await self.get_latest_schema(artifact_group_id, artifact_id)
                    
                    # Extract schema content and generate fingerprint
                    if isinstance(schema, dict) and "schema" in schema:
                        schema_content_str = schema["schema"]
                    else:
                        schema_content_str = json.dumps(schema)
                    
                    artifact_schema = json.loads(schema_content_str)
                    artifact_canonical = to_parsing_canonical_form(artifact_schema)
                    artifact_fingerprint = fingerprint(artifact_canonical, algorithm='md5')
                    
                    # Compare fingerprints
                    if artifact_fingerprint == target_fingerprint:
                        return artifact_group_id, artifact_id
                        
                except Exception:
                    # Skip artifacts that fail fingerprint comparison
                    continue
            
            return None
            
        except httpx.HTTPStatusError as e:
            raise AvroCurioError(f"HTTP error {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise AvroCurioError(f"Request error: {e}")
        except json.JSONDecodeError as e:
            raise AvroCurioError(f"Invalid JSON schema content: {e}")