"""Tests for custom exceptions."""

import pytest
from avrocurio.exceptions import (
    AvroCurioError,
    SchemaNotFoundError,
    InvalidWireFormatError,
    SerializationError,
    DeserializationError,
    SchemaMismatchError,
)


class TestExceptionHierarchy:
    """Test custom exception hierarchy."""
    
    def test_base_exception(self):
        """Test base AvroCurioError exception."""
        exc = AvroCurioError("Base error")
        assert str(exc) == "Base error"
        assert isinstance(exc, Exception)
    
    def test_schema_not_found_error(self):
        """Test SchemaNotFoundError inheritance."""
        exc = SchemaNotFoundError("Schema not found")
        assert str(exc) == "Schema not found"
        assert isinstance(exc, AvroCurioError)
        assert isinstance(exc, Exception)
    
    def test_invalid_wire_format_error(self):
        """Test InvalidWireFormatError inheritance."""
        exc = InvalidWireFormatError("Invalid format")
        assert str(exc) == "Invalid format"
        assert isinstance(exc, AvroCurioError)
        assert isinstance(exc, Exception)
    
    def test_serialization_error(self):
        """Test SerializationError inheritance."""
        exc = SerializationError("Serialization failed")
        assert str(exc) == "Serialization failed"
        assert isinstance(exc, AvroCurioError)
        assert isinstance(exc, Exception)
    
    def test_deserialization_error(self):
        """Test DeserializationError inheritance."""
        exc = DeserializationError("Deserialization failed")
        assert str(exc) == "Deserialization failed"
        assert isinstance(exc, AvroCurioError)
        assert isinstance(exc, Exception)
    
    def test_schema_mismatch_error(self):
        """Test SchemaMismatchError inheritance."""
        exc = SchemaMismatchError("Schema mismatch")
        assert str(exc) == "Schema mismatch"
        assert isinstance(exc, AvroCurioError)
        assert isinstance(exc, Exception)
    
    def test_exception_catching(self):
        """Test that specific exceptions can be caught as base class."""
        specific_exceptions = [
            SchemaNotFoundError("test"),
            InvalidWireFormatError("test"),
            SerializationError("test"),
            DeserializationError("test"),
            SchemaMismatchError("test"),
        ]
        
        for exc in specific_exceptions:
            try:
                raise exc
            except AvroCurioError:
                # Should be caught as base class
                pass
            else:
                pytest.fail(f"{type(exc).__name__} not caught as AvroCurioError")
    
    def test_exception_with_cause(self):
        """Test exception chaining."""
        original_error = ValueError("Original error")
        
        try:
            raise original_error
        except ValueError as e:
            try:
                raise SerializationError("Serialization failed") from e
            except SerializationError as chained_error:
                assert chained_error.__cause__ is original_error