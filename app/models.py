"""
SQLAlchemy ORM models — mirrors the schema from the assignment exactly.

Tables:
  - 6 lookup tables (store_brands, store_types, cities, states, countries, regions)
  - stores
  - users
  - permanent_journey_plans (store-user mapping / PJP)
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Date, DateTime,
    ForeignKey, UniqueConstraint, CheckConstraint
)
from sqlalchemy.orm import relationship
from app.database import Base


# ---------------------------------------------------------------------------
# Lookup Tables (all follow the same pattern: id + unique name)
# ---------------------------------------------------------------------------

class StoreBrand(Base):
    __tablename__ = "store_brands"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)


class StoreType(Base):
    __tablename__ = "store_types"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)


class City(Base):
    __tablename__ = "cities"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)


class State(Base):
    __tablename__ = "states"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)


class Country(Base):
    __tablename__ = "countries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)


class Region(Base):
    __tablename__ = "regions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)


# Convenient mapping for dynamic access
LOOKUP_MODELS = {
    "store_brand": StoreBrand,
    "store_type": StoreType,
    "city": City,
    "state": State,
    "country": Country,
    "region": Region,
}


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------

class Store(Base):
    __tablename__ = "stores"

    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(String(255), nullable=False, unique=True)
    store_external_id = Column(String(255), default="")
    name = Column(String(255), nullable=False)
    title = Column(String(255), nullable=False)
    store_brand_id = Column(Integer, ForeignKey("store_brands.id", ondelete="SET NULL"), nullable=True)
    store_type_id = Column(Integer, ForeignKey("store_types.id", ondelete="SET NULL"), nullable=True)
    city_id = Column(Integer, ForeignKey("cities.id", ondelete="SET NULL"), nullable=True)
    state_id = Column(Integer, ForeignKey("states.id", ondelete="SET NULL"), nullable=True)
    country_id = Column(Integer, ForeignKey("countries.id", ondelete="SET NULL"), nullable=True)
    region_id = Column(Integer, ForeignKey("regions.id", ondelete="SET NULL"), nullable=True)
    latitude = Column(Float, default=0.0)
    longitude = Column(Float, default=0.0)
    is_active = Column(Boolean, default=True)
    created_on = Column(DateTime, default=datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(150), nullable=False, unique=True)
    first_name = Column(String(150), default="")
    last_name = Column(String(150), default="")
    email = Column(String(254), nullable=False)
    user_type = Column(Integer, default=1)
    phone_number = Column(String(32), default="")
    supervisor_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    is_active = Column(Boolean, default=True)
    created_on = Column(DateTime, default=datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        CheckConstraint("user_type IN (1, 2, 3, 7)", name="ck_user_type"),
    )


# ---------------------------------------------------------------------------
# Permanent Journey Plans (Store-User Mapping)
# ---------------------------------------------------------------------------

class PermanentJourneyPlan(Base):
    __tablename__ = "permanent_journey_plans"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=True)
    is_active = Column(Boolean, default=True)
    created_on = Column(DateTime, default=datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("user_id", "store_id", "date", name="uq_user_store_date"),
    )
