"""Confluent Schema Registry wire format handling."""

import struct

from .exceptions import InvalidWireFormatError


class ConfluentWireFormat:
    """
    Handles Confluent Schema Registry wire format encoding/decoding.

    The wire format consists of:
    - Magic byte (0x0) - 1 byte
    - Schema ID - 4 bytes (big-endian)
    - Avro payload - remaining bytes
    """

    MAGIC_BYTE = 0x0
    SCHEMA_ID_SIZE = 4
    HEADER_SIZE = 1 + SCHEMA_ID_SIZE  # magic byte + schema ID

    @staticmethod
    def encode(schema_id: int, payload: bytes) -> bytes:
        """
        Encode payload with Confluent wire format.

        Args:
            schema_id: Schema ID to embed in the message
            payload: Serialized Avro data

        Returns:
            Message with Confluent wire format framing

        """
        # Pack magic byte (1 byte) + schema ID (4 bytes, big-endian)
        header = struct.pack(">BI", ConfluentWireFormat.MAGIC_BYTE, schema_id)
        return header + payload

    @staticmethod
    def decode(message: bytes) -> tuple[int, bytes]:
        """
        Decode message with Confluent wire format.

        Args:
            message: Message with Confluent wire format framing

        Returns:
            Tuple of (schema_id, payload)

        Raises:
            InvalidWireFormatError: If message doesn't follow Confluent wire format

        """
        ConfluentWireFormat.validate_magic_byte(message)

        if len(message) < ConfluentWireFormat.HEADER_SIZE:
            msg = f"Message too short. Expected at least {ConfluentWireFormat.HEADER_SIZE} bytes, got {len(message)}"
            raise InvalidWireFormatError(msg)

        # Unpack magic byte (1 byte) + schema ID (4 bytes, big-endian)
        _magic_byte, schema_id = struct.unpack(">BI", message[: ConfluentWireFormat.HEADER_SIZE])
        payload = message[ConfluentWireFormat.HEADER_SIZE :]

        return schema_id, payload

    @staticmethod
    def validate_magic_byte(message: bytes) -> None:
        """
        Validate that message starts with correct magic byte.

        Args:
            message: Message to validate

        Raises:
            InvalidWireFormatError: If magic byte is incorrect or missing

        """
        if len(message) < 1:
            raise InvalidWireFormatError("Message is empty")

        magic_byte = message[0]
        if magic_byte != ConfluentWireFormat.MAGIC_BYTE:
            msg = f"Invalid magic byte. Expected {ConfluentWireFormat.MAGIC_BYTE!r}, got {magic_byte!r}"
            raise InvalidWireFormatError(msg)
