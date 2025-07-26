"""Schema evolution compatibility integration tests."""

import enum
import json
from dataclasses import dataclass

import pytest
from dataclasses_avroschema import AvroModel

from avrocurio import ApicurioClient, AvroSerializer
from avrocurio.exceptions import SchemaNotFoundError


# Enum evolution test models
class StatusV1(enum.Enum):
    """Version 1 of status enum."""

    UNKNOWN = "UNKNOWN"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"

    @enum.nonmember
    class Meta:
        schema_name = "Status"
        default = "UNKNOWN"


class StatusV2(enum.Enum):
    """Version 2 of status enum - adds new member (backward compatible)."""

    UNKNOWN = "UNKNOWN"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    PENDING = "PENDING"  # New member

    @enum.nonmember
    class Meta:
        schema_name = "Status"
        default = "UNKNOWN"


class StatusV3(enum.Enum):
    """Version 3 of status enum - removes member (forward compatible with defaults)."""

    UNKNOWN = "UNKNOWN"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    # PENDING removed

    @enum.nonmember
    class Meta:
        schema_name = "Status"
        default = "UNKNOWN"


# Record evolution test models
@dataclass
class UserV1(AvroModel):
    """Version 1 - Base user schema."""

    name: str
    email: str
    status: StatusV1 = StatusV1.UNKNOWN

    class Meta:
        schema_name = "User"


@dataclass
class UserV2(AvroModel):
    """Version 2 - Backward compatible (adds optional fields)."""

    name: str
    email: str
    status: StatusV2 = StatusV2.UNKNOWN
    age: int = 25  # Default value enables backward compatibility
    is_active: bool = True

    class Meta:
        schema_name = "User"


@dataclass
class UserV3(AvroModel):
    """Version 3 - Type promotion and enum evolution."""

    name: str
    email: str
    status: StatusV3 = StatusV3.UNKNOWN  # PENDING no longer available
    age: int = 25
    is_active: bool = True
    score: float = 0.0  # Type promotion: was int, now float

    class Meta:
        schema_name = "User"


@dataclass
class UserIncompatible(AvroModel):
    """Incompatible version - removes required field."""

    name: str
    # email removed - this should cause errors
    status: StatusV1 = StatusV1.ACTIVE

    class Meta:
        schema_name = "User"


@pytest.mark.integration
class TestSchemaEvolution:
    """Test schema evolution patterns with real Apicurio Registry."""

    @classmethod
    def setup_class(cls):
        """
        Verify that the Avro models we use for assertions render the expected Avro schemas.

        Running this as a setup class ensures all tests fail if the schemas are not as expected, which may avoid being confused by a red herring.
        """
        assert json.loads(UserV1.avro_schema()) == {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["UNKNOWN", "ACTIVE", "INACTIVE"],
                        "default": "UNKNOWN",
                        "doc": "Version 1 of status enum.",
                    },
                    "default": "UNKNOWN",
                },
            ],
            "doc": "Version 1 - Base user schema.",
        }
        assert json.loads(UserV2.avro_schema()) == {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["UNKNOWN", "ACTIVE", "INACTIVE", "PENDING"],
                        "default": "UNKNOWN",
                        "doc": "Version 2 of status enum - adds new member (backward compatible).",
                    },
                    "default": "UNKNOWN",
                },
                {
                    "default": 25,
                    "name": "age",
                    "type": "long",
                },
                {
                    "default": True,
                    "name": "is_active",
                    "type": "boolean",
                },
            ],
            "doc": "Version 2 - Backward compatible (adds optional fields).",
        }
        assert json.loads(UserV3.avro_schema()) == {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
                {
                    "name": "status",
                    "type": {
                        "type": "enum",
                        "name": "Status",
                        "symbols": ["UNKNOWN", "ACTIVE", "INACTIVE"],
                        "default": "UNKNOWN",
                        "doc": "Version 3 of status enum - removes member (forward compatible with defaults).",
                    },
                    "default": "UNKNOWN",
                },
                {
                    "default": 25,
                    "name": "age",
                    "type": "long",
                },
                {
                    "default": True,
                    "name": "is_active",
                    "type": "boolean",
                },
                {
                    "default": 0.0,
                    "name": "score",
                    "type": "double",
                },
            ],
            "doc": "Version 3 - Type promotion and enum evolution.",
        }

    async def test_backward_compatibility_record_fields(
        self,
        serializer: AvroSerializer,
        apicurio_client: ApicurioClient,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test that new schema can read data written with old schema (backward compatibility)."""
        # Create data with V1 schema
        user_v1 = UserV1(
            name="John Doe", email="john@example.com", status=StatusV1.ACTIVE
        )

        # Register V1 schema and serialize data
        artifact_id = f"{test_artifact_id}-backward-compat"
        schema_v1_content = user_v1.avro_schema()

        global_id_v1 = await apicurio_client.register_schema(
            group_id=test_group_id,
            artifact_id=artifact_id,
            schema_content=schema_v1_content,
        )

        serialized_v1 = await serializer.serialize(user_v1, global_id_v1)

        # Register V2 schema (backward compatible - adds optional fields)
        schema_v2_content = UserV2.avro_schema()
        await apicurio_client.register_schema_version(
            group_id=test_group_id,
            artifact_id=artifact_id,
            schema_content=schema_v2_content,
        )

        # Deserialize V1 data using V2 schema - should work with defaults
        deserialized_v2 = await serializer.deserialize(serialized_v1, UserV2)

        # Verify original fields preserved
        assert deserialized_v2.name == "John Doe"
        assert deserialized_v2.email == "john@example.com"
        assert deserialized_v2.status == StatusV2.ACTIVE

        # Verify default values applied for new fields
        assert deserialized_v2.age == 25  # Default value
        assert deserialized_v2.is_active is True  # Default value

    async def test_forward_compatibility_record_fields(
        self,
        serializer: AvroSerializer,
        apicurio_client: ApicurioClient,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test that old schema can read data written with new schema (forward compatibility)."""
        # For true forward compatibility, we need to use compatible enum values
        user_v2 = UserV2(
            name="Jane Smith",
            email="jane@example.com",
            status=StatusV2.PENDING,  # Use compatible enum value
            age=30,
            is_active=False,
        )

        # Register V2 schema and serialize data
        artifact_id = f"{test_artifact_id}-forward-compat"
        schema_v2_content = user_v2.avro_schema()
        global_id_v2 = await apicurio_client.register_schema(
            group_id=test_group_id,
            artifact_id=artifact_id,
            schema_content=schema_v2_content,
        )

        serialized_v2 = await serializer.serialize(user_v2, global_id_v2)
        deserialized_v1 = await serializer.deserialize(serialized_v2, UserV1)

        # Verify all fields preserved
        assert deserialized_v1.name == "Jane Smith"
        assert deserialized_v1.email == "jane@example.com"
        assert deserialized_v1.status == StatusV1.UNKNOWN

    async def test_enum_evolution_add_member(
        self,
        serializer: AvroSerializer,
        apicurio_client: ApicurioClient,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test adding enum members (backward compatible)."""
        # Create data with V1 enum
        user_v1 = UserV1(
            name="Active User", email="active@example.com", status=StatusV1.ACTIVE
        )

        # Register V1 schema and serialize
        artifact_id = f"{test_artifact_id}-enum-add"
        schema_v1_content = user_v1.avro_schema()
        global_id_v1 = await apicurio_client.register_schema(
            group_id=test_group_id,
            artifact_id=artifact_id,
            schema_content=schema_v1_content,
        )

        serialized_v1 = await serializer.serialize(user_v1, global_id_v1)

        # Register V2 schema with expanded enum
        schema_v2_content = UserV2.avro_schema()
        global_id_v2 = await apicurio_client.register_schema_version(
            group_id=test_group_id,
            artifact_id=artifact_id,
            schema_content=schema_v2_content,
        )

        # Deserialize V1 data with V2 schema (expanded enum)
        deserialized_v2 = await serializer.deserialize(serialized_v1, UserV2)

        assert deserialized_v2.status == StatusV2.ACTIVE
        assert deserialized_v2.name == "Active User"

        # Test new enum value works
        user_v2_pending = UserV2(
            name="Pending User", email="pending@example.com", status=StatusV2.PENDING
        )
        serialized_pending = await serializer.serialize(user_v2_pending, global_id_v2)
        deserialized_pending = await serializer.deserialize(serialized_pending, UserV2)
        assert deserialized_pending.status == StatusV2.PENDING

    async def test_type_promotion_compatibility(
        self,
        serializer: AvroSerializer,
        apicurio_client: ApicurioClient,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test Avro type promotion"""

        # Create a simple model for type promotion testing
        @dataclass
        class ScoreV1(AvroModel):
            """Version with int score."""

            user_id: str
            score: int

            class Meta:
                schema_name = "Score"

        @dataclass
        class ScoreV2(AvroModel):
            """Version with float score (promoted from int)."""

            user_id: str
            score: float

            class Meta:
                schema_name = "Score"

        # Register V1 schema and serialize
        artifact_id = f"{test_artifact_id}-type-promotion"
        schema_v1_content = ScoreV1.avro_schema()
        global_id_v1 = await apicurio_client.register_schema(
            group_id=test_group_id,
            artifact_id=artifact_id,
            schema_content=schema_v1_content,
        )

        # Create data with int score
        score_v1 = ScoreV1(user_id="user123", score=85)
        serialized_v1 = await serializer.serialize(score_v1, global_id_v1)

        # Register V2 schema with promoted type
        schema_v2_content = ScoreV2.avro_schema()
        global_id_v2 = await apicurio_client.register_schema_version(
            group_id=test_group_id,
            artifact_id=artifact_id,
            schema_content=schema_v2_content,
        )

        # Demonstrate that V1 data (int) can be deserialized with V2 schema (float)
        deserialized_v2 = await serializer.deserialize(serialized_v1, ScoreV2)

        assert deserialized_v2.user_id == "user123"
        assert deserialized_v2.score == 85

        # Demonstrate that the V2 schema can encode/decode float values
        score_v2_new = ScoreV2(user_id="user456", score=92.5)
        serialized_v2_new = await serializer.serialize(score_v2_new, global_id_v2)
        deserialized_v2_new = await serializer.deserialize(serialized_v2_new, ScoreV2)

        assert isinstance(deserialized_v2_new.score, float)
        assert deserialized_v2_new.score == 92.5

    async def test_full_evolution_chain(
        self,
        serializer: AvroSerializer,
        apicurio_client: ApicurioClient,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test complete evolution chain V1 → V2 showing backward compatibility."""
        artifact_id = f"{test_artifact_id}-full-evolution"

        # Step 1: Create and serialize V1 data
        user_v1 = UserV1(
            name="Evolution Test", email="evolution@example.com", status=StatusV1.ACTIVE
        )

        schema_v1_content = user_v1.avro_schema()
        global_id_v1 = await apicurio_client.register_schema(
            group_id=test_group_id,
            artifact_id=artifact_id,
            schema_content=schema_v1_content,
        )
        serialized_v1 = await serializer.serialize(user_v1, global_id_v1)

        # Step 2: Evolve to V2, read V1 data
        schema_v2_content = UserV2.avro_schema()
        global_id_v2 = await apicurio_client.register_schema_version(
            group_id=test_group_id,
            artifact_id=artifact_id,
            schema_content=schema_v2_content,
        )

        # V1 data read by V2 schema - demonstrates backward compatibility
        v1_data_as_v2 = await serializer.deserialize(serialized_v1, UserV2)
        assert v1_data_as_v2.name == "Evolution Test"
        assert v1_data_as_v2.age == 25  # Default value
        assert v1_data_as_v2.is_active is True  # Default value
        assert v1_data_as_v2.status == StatusV2.ACTIVE  # Enum value preserved

        # Step 3: Create V2 data with compatible enum values
        user_v2 = UserV2(
            name="V2 User",
            email="v2@example.com",
            status=StatusV2.ACTIVE,  # Use compatible enum value
            age=35,
            is_active=True,
        )
        serialized_v2 = await serializer.serialize(user_v2, global_id_v2)

        # Verify V2 data can be read back correctly
        v2_data_verified = await serializer.deserialize(serialized_v2, UserV2)
        assert v2_data_verified.name == "V2 User"
        assert v2_data_verified.age == 35
        assert v2_data_verified.status == StatusV2.ACTIVE

    async def test_enum_value_removal(
        self,
        serializer: AvroSerializer,
        apicurio_client: ApicurioClient,
        test_group_id: str,
        test_artifact_id: str,
    ):
        """Test that the default enum value is used when an unknown member is encountered."""
        user_v2 = UserV2(
            name="Pending User",
            email="pending@example.com",
            status=StatusV2.PENDING,  # This value doesn't exist in V1 or V3
            age=30,
        )

        # Register V2 schema and serialize
        artifact_id = f"{test_artifact_id}-enum-removal"
        schema_v2_content = user_v2.avro_schema()
        global_id_v2 = await apicurio_client.register_schema(
            group_id=test_group_id,
            artifact_id=artifact_id,
            schema_content=schema_v2_content,
        )
        serialized_v2 = await serializer.serialize(user_v2, global_id_v2)

        result = await serializer.deserialize(serialized_v2, UserV1)
        assert result.status == StatusV1.UNKNOWN

    async def test_schema_not_found_error(self, apicurio_client: ApicurioClient):
        """Test error handling for missing schemas (inherited from base tests)."""
        non_existent_id = 999999999

        with pytest.raises(
            SchemaNotFoundError,
            match=f"Schema with global ID {non_existent_id} not found",
        ):
            await apicurio_client.get_schema_by_global_id(non_existent_id)
