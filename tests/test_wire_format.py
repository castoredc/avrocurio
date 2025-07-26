"""Tests for wire format handling."""

import pytest

from avrocurio.exceptions import InvalidWireFormatError
from avrocurio.wire_format import ConfluentWireFormat


class TestConfluentWireFormat:
    """Test cases for ConfluentWireFormat class."""

    def test_encode_basic(self):
        """Test basic encoding with magic byte and schema ID."""
        schema_id = 12345
        payload = b"test payload"

        result = ConfluentWireFormat.encode(schema_id, payload)

        # Should start with magic byte (0x0)
        assert result[0] == 0x0
        # Total length should be 1 (magic) + 4 (schema_id) + payload length
        assert len(result) == 5 + len(payload)
        # Should end with original payload
        assert result[5:] == payload

    def test_encode_different_schema_ids(self):
        """Test encoding with different schema IDs."""
        payload = b"test"

        # Test small schema ID
        result1 = ConfluentWireFormat.encode(1, payload)
        assert result1[0] == 0x0

        # Test large schema ID
        result2 = ConfluentWireFormat.encode(2147483647, payload)  # Max int32
        assert result2[0] == 0x0

        # Results should be different due to different schema IDs
        assert result1[1:5] != result2[1:5]

    def test_decode_basic(self):
        """Test basic decoding of wire format message."""
        schema_id = 12345
        payload = b"test payload"

        # Encode first
        encoded = ConfluentWireFormat.encode(schema_id, payload)

        # Decode and verify
        decoded_schema_id, decoded_payload = ConfluentWireFormat.decode(encoded)
        assert decoded_schema_id == schema_id
        assert decoded_payload == payload

    def test_decode_empty_payload(self):
        """Test decoding with empty payload."""
        schema_id = 999
        payload = b""

        encoded = ConfluentWireFormat.encode(schema_id, payload)
        decoded_schema_id, decoded_payload = ConfluentWireFormat.decode(encoded)

        assert decoded_schema_id == schema_id
        assert decoded_payload == payload

    def test_decode_invalid_magic_byte(self):
        """Test decoding fails with invalid magic byte."""
        # Create message with wrong magic byte
        invalid_message = b"\x01\x00\x00\x30\x39test"

        with pytest.raises(InvalidWireFormatError, match="Invalid magic byte"):
            ConfluentWireFormat.decode(invalid_message)

    def test_decode_message_too_short(self):
        """Test decoding fails with message too short."""
        # Message with only magic byte
        short_message = b"\x00"

        with pytest.raises(InvalidWireFormatError, match="Message too short"):
            ConfluentWireFormat.decode(short_message)

        # Message with magic byte + partial schema ID
        partial_message = b"\x00\x00\x00"

        with pytest.raises(InvalidWireFormatError, match="Message too short"):
            ConfluentWireFormat.decode(partial_message)

    def test_decode_empty_message(self):
        """Test decoding fails with empty message."""
        with pytest.raises(InvalidWireFormatError, match="Message is empty"):
            ConfluentWireFormat.decode(b"")

    def test_validate_magic_byte_valid(self):
        """Test magic byte validation with valid message."""
        valid_message = b"\x00\x00\x00\x30\x39test"

        # Should not raise exception
        ConfluentWireFormat.validate_magic_byte(valid_message)

    def test_validate_magic_byte_invalid(self):
        """Test magic byte validation with invalid message."""
        invalid_message = b"\x01\x00\x00\x30\x39test"

        with pytest.raises(InvalidWireFormatError, match="Invalid magic byte"):
            ConfluentWireFormat.validate_magic_byte(invalid_message)

    def test_validate_magic_byte_empty(self):
        """Test magic byte validation with empty message."""
        with pytest.raises(InvalidWireFormatError, match="Message is empty"):
            ConfluentWireFormat.validate_magic_byte(b"")

    def test_round_trip_encoding(self):
        """Test encode/decode round trip preserves data."""
        test_cases = [
            (1, b"simple"),
            (999999, b"complex payload with \x00 null bytes"),
            (0, b""),
            (2147483647, b"max schema id test"),
        ]

        for schema_id, payload in test_cases:
            encoded = ConfluentWireFormat.encode(schema_id, payload)
            decoded_schema_id, decoded_payload = ConfluentWireFormat.decode(encoded)

            assert decoded_schema_id == schema_id
            assert decoded_payload == payload

    def test_constants(self):
        """Test class constants are correct."""
        assert ConfluentWireFormat.MAGIC_BYTE == 0x0
        assert ConfluentWireFormat.SCHEMA_ID_SIZE == 4
        assert ConfluentWireFormat.HEADER_SIZE == 5  # 1 + 4
