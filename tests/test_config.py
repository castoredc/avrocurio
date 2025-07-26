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

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            (
                {
                    "base_url": "https://registry.example.com",
                    "timeout": 60.0,
                    "max_retries": 5,
                    "auth": ("username", "password"),
                },
                {
                    "base_url": "https://registry.example.com",
                    "timeout": 60.0,
                    "max_retries": 5,
                    "auth": ("username", "password"),
                },
            ),
            (
                {"base_url": "https://custom.registry.com", "timeout": 45.0},
                {"base_url": "https://custom.registry.com", "timeout": 45.0, "max_retries": 3, "auth": None},
            ),
            (
                {"auth": ("user", "pass")},
                {"base_url": "http://localhost:8080", "timeout": 30.0, "max_retries": 3, "auth": ("user", "pass")},
            ),
        ],
    )
    def test_configuration_values(self, kwargs, expected):
        """Test configuration with various value combinations."""
        config = ApicurioConfig(**kwargs)

        assert config.base_url == expected["base_url"]
        assert config.timeout == expected["timeout"]
        assert config.max_retries == expected["max_retries"]
        assert config.auth == expected["auth"]

    def test_equality_and_inequality(self):
        """Test configuration equality and inequality."""
        config1 = ApicurioConfig(base_url="http://test.com", timeout=30.0, max_retries=3, auth=("user", "pass"))
        config2 = ApicurioConfig(base_url="http://test.com", timeout=30.0, max_retries=3, auth=("user", "pass"))
        config3 = ApicurioConfig(base_url="http://different.com", timeout=30.0, max_retries=3, auth=("user", "pass"))

        assert config1 == config2
        assert config1 != config3

    def test_repr_contains_key_info(self):
        """Test configuration string representation."""
        config = ApicurioConfig(base_url="http://test.com", auth=("user", "pass"))

        repr_str = repr(config)
        assert "ApicurioConfig" in repr_str
        assert "base_url='http://test.com'" in repr_str
        assert "auth=('user', 'pass')" in repr_str

    def test_dataclass_mutability(self):
        """Test that config can be modified after creation."""
        config = ApicurioConfig()
        config.base_url = "http://modified.com"
        assert config.base_url == "http://modified.com"

    @pytest.mark.parametrize(
        ("field", "expected_type"),
        [
            ("base_url", str),
            ("timeout", float),
            ("max_retries", int),
        ],
    )
    def test_field_types(self, field, expected_type):
        """Test that fields have expected types."""
        config = ApicurioConfig()
        assert isinstance(getattr(config, field), expected_type)

    def test_auth_field_type(self):
        """Test auth field type handling."""
        config_no_auth = ApicurioConfig()
        config_with_auth = ApicurioConfig(auth=("user", "pass"))

        assert config_no_auth.auth is None
        assert isinstance(config_with_auth.auth, tuple)
        assert len(config_with_auth.auth) == 2
