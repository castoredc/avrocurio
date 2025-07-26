"""Configuration classes for AvroCurio library."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ApicurioConfig:
    """Configuration for Apicurio Schema Registry client.
    
    Args:
        base_url: Base URL of the Apicurio Registry instance
        timeout: HTTP request timeout in seconds
        max_retries: Maximum number of retry attempts for failed requests
        auth: Optional basic authentication tuple (username, password)
    """
    base_url: str = "http://localhost:8080"
    timeout: float = 30.0
    max_retries: int = 3
    auth: Optional[tuple[str, str]] = None