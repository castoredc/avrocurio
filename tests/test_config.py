"""Tests for configuration classes."""

import pytest
from avrocurio.config import ApicurioConfig


class TestApicurioConfig:
    """Test cases for ApicurioConfig dataclass."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = ApicurioConfig()
        
        assert config.base_url == "http://localhost:8080"
        assert config.timeout == 30.0
        assert config.max_retries == 3
        assert config.auth is None
    
    def test_custom_values(self):
        """Test configuration with custom values."""
        config = ApicurioConfig(
            base_url="https://registry.example.com",
            timeout=60.0,
            max_retries=5,
            auth=("username", "password")
        )
        
        assert config.base_url == "https://registry.example.com"
        assert config.timeout == 60.0
        assert config.max_retries == 5
        assert config.auth == ("username", "password")
    
    def test_partial_custom_values(self):
        """Test configuration with some custom values."""
        config = ApicurioConfig(
            base_url="https://custom.registry.com",
            timeout=45.0
        )
        
        assert config.base_url == "https://custom.registry.com"
        assert config.timeout == 45.0
        assert config.max_retries == 3  # default
        assert config.auth is None  # default
    
    def test_auth_tuple(self):
        """Test authentication tuple configuration."""
        config = ApicurioConfig(auth=("user", "pass"))
        
        assert config.auth is not None
        assert len(config.auth) == 2
        assert config.auth[0] == "user"
        assert config.auth[1] == "pass"
    
    def test_equality(self):
        """Test configuration equality."""
        config1 = ApicurioConfig(
            base_url="http://test.com",
            timeout=30.0,
            max_retries=3,
            auth=("user", "pass")
        )
        
        config2 = ApicurioConfig(
            base_url="http://test.com",
            timeout=30.0,
            max_retries=3,
            auth=("user", "pass")
        )
        
        config3 = ApicurioConfig(
            base_url="http://different.com",
            timeout=30.0,
            max_retries=3,
            auth=("user", "pass")
        )
        
        assert config1 == config2
        assert config1 != config3
    
    def test_repr(self):
        """Test configuration string representation."""
        config = ApicurioConfig(
            base_url="http://test.com",
            auth=("user", "pass")
        )
        
        repr_str = repr(config)
        assert "ApicurioConfig" in repr_str
        assert "base_url='http://test.com'" in repr_str
        assert "auth=('user', 'pass')" in repr_str
    
    def test_dataclass_immutable_after_creation(self):
        """Test that config can be modified after creation (dataclass is mutable by default)."""
        config = ApicurioConfig()
        
        # Should be able to modify
        config.base_url = "http://modified.com"
        assert config.base_url == "http://modified.com"
    
    def test_type_annotations(self):
        """Test that type annotations are preserved."""
        config = ApicurioConfig()
        
        # Check that the fields have the expected types
        assert isinstance(config.base_url, str)
        assert isinstance(config.timeout, float)
        assert isinstance(config.max_retries, int)
        assert config.auth is None or isinstance(config.auth, tuple)