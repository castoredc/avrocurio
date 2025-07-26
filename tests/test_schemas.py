"""Test schema classes for AvroCurio tests."""

from dataclasses import dataclass

from dataclasses_avroschema import AvroModel


@dataclass
class SimpleUser(AvroModel):
    """Simple user schema for testing."""

    name: str
    age: int


@dataclass
class ComplexUser(AvroModel):
    """Complex user schema with optional fields."""

    name: str
    age: int
    email: str | None = None
    is_active: bool = True


@dataclass
class Product(AvroModel):
    """Product schema for testing."""

    id: int
    name: str
    price: float
    category: str
