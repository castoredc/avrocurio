"""Configuration classes for AvroCurio library."""

from dataclasses import dataclass


@dataclass
class ApicurioConfig:
    """
    Configuration for Apicurio Schema Registry client.

    Args:
        base_url: Base URL of the Apicurio Registry instance
        timeout: HTTP request timeout in seconds
        max_retries: Maximum number of retry attempts for failed requests
        auth: Optional basic authentication tuple (username, password)
        schema_cache_size: Maximum number of schemas to cache (0 disables caching)
        failed_lookup_cache_size: Maximum number of failed lookups to cache (0 disables caching)
        failed_lookup_cache_ttl: TTL in seconds for failed lookup cache entries

    """

    base_url: str = "http://localhost:8080"
    timeout: float = 30.0
    max_retries: int = 3
    auth: tuple[str, str] | None = None
    schema_cache_size: int = 1000
    failed_lookup_cache_size: int = 100
    failed_lookup_cache_ttl: int = 300
