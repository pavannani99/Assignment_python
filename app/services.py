"""
Business logic for CSV ingestion:
  - CSV parsing (streaming / chunked)
  - Data normalization
  - Batch get-or-create for lookup tables
  - Bulk inserts for stores, users, mappings
  - Error collection and reporting
"""

import csv
import io
import re
import time
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy import select

from app.models import (
    Store, User, PermanentJourneyPlan,
    StoreBrand, StoreType, City, State, Country, Region,
    LOOKUP_MODELS,
)
from app.validators import (
    validate_store_row,
    validate_user_row,
    validate_mapping_row,
    _normalize,
)


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def normalize_lookup_value(value: str) -> str:
    """
    Normalize a lookup value:
      1. Strip whitespace
      2. Collapse internal whitespace ("Reliance  Fresh" -> "Reliance Fresh")
      3. Title-case ("INDIA" -> "India", "mumbai" -> "Mumbai")
    """
    if not value:
        return ""
    cleaned = re.sub(r"\s+", " ", value.strip())
    return cleaned.title() if cleaned else ""


def parse_bool(value: str) -> bool:
    """Parse a boolean from a string."""
    return _normalize(value).lower() in ("true", "1", "yes", "")


# ---------------------------------------------------------------------------
# Batch get-or-create for lookup tables
# ---------------------------------------------------------------------------

def batch_get_or_create(db: Session, model_class, names: set, cache: dict) -> dict:
    """
    For a set of normalized names, look up existing records and create missing ones.
    Returns a dict mapping name -> id.
    Updates the cache in-place for future batches.
    """
    # Filter out already-cached and empty names
    to_lookup = {n for n in names if n and n not in cache}

    if not to_lookup:
        return cache

    # Query existing
    existing = db.execute(
        select(model_class).where(model_class.name.in_(to_lookup))
    ).scalars().all()

    for record in existing:
        cache[record.name] = record.id
        to_lookup.discard(record.name)

    # Bulk-create missing
    if to_lookup:
        new_objects = [model_class(name=n) for n in to_lookup]
        db.bulk_save_objects(new_objects, return_defaults=True)
        db.flush()
        # Re-query to get IDs (bulk_save_objects with return_defaults may not work on all backends)
        created = db.execute(
            select(model_class).where(model_class.name.in_(to_lookup))
        ).scalars().all()
        for record in created:
            cache[record.name] = record.id

    return cache


# ---------------------------------------------------------------------------
# Store ingestion
# ---------------------------------------------------------------------------

def ingest_stores(db: Session, file_content: bytes) -> dict:
    """
    Parse, validate, and ingest a stores CSV file.
    Returns a result dict with summary and error details.
    """
    start_time = time.time()

    text = file_content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))

    # Pre-load existing store_ids from DB to check uniqueness
    existing_store_ids = set(
        r[0] for r in db.execute(select(Store.store_id)).all()
    )

    # Lookup caches (name -> id)
    caches = {key: {} for key in LOOKUP_MODELS}

    all_errors = []
    valid_rows = []
    seen_store_ids = set()
    total_rows = 0

    for row_num_0based, row in enumerate(reader):
        row_num = row_num_0based + 2  # +2 because row 1 is header, csv is 0-indexed
        total_rows += 1

        # Validate
        errors = validate_store_row(row, row_num, seen_store_ids, existing_store_ids)
        if errors:
            all_errors.extend(errors)
            continue

        # Normalize fields
        store_id = _normalize(row.get("store_id", ""))
        seen_store_ids.add(store_id)

        valid_rows.append({
            "store_id": store_id,
            "store_external_id": _normalize(row.get("store_external_id", "")),
            "name": _normalize(row.get("name", "")),
            "title": _normalize(row.get("title", "")),
            "store_brand": normalize_lookup_value(row.get("store_brand", "")),
            "store_type": normalize_lookup_value(row.get("store_type", "")),
            "city": normalize_lookup_value(row.get("city", "")),
            "state": normalize_lookup_value(row.get("state", "")),
            "country": normalize_lookup_value(row.get("country", "")),
            "region": normalize_lookup_value(row.get("region", "")),
            "latitude": float(_normalize(row.get("latitude", "0")) or "0"),
            "longitude": float(_normalize(row.get("longitude", "0")) or "0"),
            "is_active": parse_bool(row.get("is_active", "true")),
        })

    # Batch resolve all lookup values
    lookup_fields = ["store_brand", "store_type", "city", "state", "country", "region"]
    for field in lookup_fields:
        unique_values = {r[field] for r in valid_rows if r[field]}
        batch_get_or_create(db, LOOKUP_MODELS[field], unique_values, caches[field])

    # Build store objects for bulk insert
    store_mappings = []
    for r in valid_rows:
        store_mappings.append({
            "store_id": r["store_id"],
            "store_external_id": r["store_external_id"],
            "name": r["name"],
            "title": r["title"],
            "store_brand_id": caches["store_brand"].get(r["store_brand"]),
            "store_type_id": caches["store_type"].get(r["store_type"]),
            "city_id": caches["city"].get(r["city"]),
            "state_id": caches["state"].get(r["state"]),
            "country_id": caches["country"].get(r["country"]),
            "region_id": caches["region"].get(r["region"]),
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "is_active": r["is_active"],
        })

    # Bulk insert
    if store_mappings:
        db.bulk_insert_mappings(Store, store_mappings)
        db.commit()

    elapsed = round(time.time() - start_time, 3)

    return {
        "file_type": "stores_master",
        "total_rows": total_rows,
        "rows_succeeded": len(store_mappings),
        "rows_failed": total_rows - len(store_mappings),
        "time_seconds": elapsed,
        "errors": all_errors,
    }


# ---------------------------------------------------------------------------
# User ingestion
# ---------------------------------------------------------------------------

def ingest_users(db: Session, file_content: bytes) -> dict:
    """
    Parse, validate, and ingest a users CSV file.
    Returns a result dict with summary and error details.
    """
    start_time = time.time()

    text = file_content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))

    # Pre-load existing usernames
    existing_usernames = set(
        r[0] for r in db.execute(select(User.username)).all()
    )

    # First pass: collect all usernames in the file for supervisor validation
    rows_list = list(reader)
    all_usernames_in_file = set()
    for row in rows_list:
        uname = _normalize(row.get("username", ""))
        if uname:
            all_usernames_in_file.add(uname)

    all_errors = []
    valid_rows = []
    seen_usernames = set()
    total_rows = 0

    for row_num_0based, row in enumerate(rows_list):
        row_num = row_num_0based + 2
        total_rows += 1

        errors = validate_user_row(row, row_num, seen_usernames, existing_usernames, all_usernames_in_file)
        if errors:
            all_errors.extend(errors)
            continue

        username = _normalize(row.get("username", ""))
        seen_usernames.add(username)

        valid_rows.append({
            "username": username,
            "first_name": _normalize(row.get("first_name", "")),
            "last_name": _normalize(row.get("last_name", "")),
            "email": _normalize(row.get("email", "")),
            "user_type": int(_normalize(row.get("user_type", "1")) or "1"),
            "phone_number": _normalize(row.get("phone_number", "")),
            "supervisor_username": _normalize(row.get("supervisor_username", "")),
            "is_active": parse_bool(row.get("is_active", "true")),
        })

    # First insert users without supervisors so they exist in DB
    user_mappings = []
    for r in valid_rows:
        user_mappings.append({
            "username": r["username"],
            "first_name": r["first_name"],
            "last_name": r["last_name"],
            "email": r["email"],
            "user_type": r["user_type"],
            "phone_number": r["phone_number"],
            "supervisor_id": None,  # resolve after insert
            "is_active": r["is_active"],
        })

    if user_mappings:
        db.bulk_insert_mappings(User, user_mappings)
        db.commit()

    # Now resolve supervisor references
    # Build username -> id map
    username_to_id = {}
    all_users = db.execute(select(User.id, User.username)).all()
    for uid, uname in all_users:
        username_to_id[uname] = uid

    # Update supervisor_id for users that have supervisors
    for r in valid_rows:
        if r["supervisor_username"]:
            supervisor_id = username_to_id.get(r["supervisor_username"])
            if supervisor_id:
                user_id = username_to_id.get(r["username"])
                if user_id:
                    db.execute(
                        User.__table__.update()
                        .where(User.__table__.c.id == user_id)
                        .values(supervisor_id=supervisor_id)
                    )
    db.commit()

    elapsed = round(time.time() - start_time, 3)

    return {
        "file_type": "users_master",
        "total_rows": total_rows,
        "rows_succeeded": len(user_mappings),
        "rows_failed": total_rows - len(user_mappings),
        "time_seconds": elapsed,
        "errors": all_errors,
    }


# ---------------------------------------------------------------------------
# Store-User Mapping (PJP) ingestion
# ---------------------------------------------------------------------------

def ingest_mapping(db: Session, file_content: bytes) -> dict:
    """
    Parse, validate, and ingest a store-user mapping CSV file.
    Returns a result dict with summary and error details.
    """
    start_time = time.time()

    text = file_content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))

    # Pre-load existing usernames and store_ids for FK validation
    existing_usernames = set(r[0] for r in db.execute(select(User.username)).all())
    existing_store_ids = set(r[0] for r in db.execute(select(Store.store_id)).all())

    # Build lookup maps: username -> user.id, store_id -> store.id
    username_to_id = dict(db.execute(select(User.username, User.id)).all())
    store_id_to_id = dict(db.execute(select(Store.store_id, Store.id)).all())

    all_errors = []
    valid_rows = []
    seen_combinations = set()
    total_rows = 0

    for row_num_0based, row in enumerate(reader):
        row_num = row_num_0based + 2
        total_rows += 1

        errors = validate_mapping_row(row, row_num, existing_usernames, existing_store_ids, seen_combinations)
        if errors:
            all_errors.extend(errors)
            continue

        username = _normalize(row.get("username", ""))
        store_id = _normalize(row.get("store_id", ""))
        date_str = _normalize(row.get("date", ""))
        combo = (username, store_id, date_str)
        seen_combinations.add(combo)

        parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        valid_rows.append({
            "user_id": username_to_id[username],
            "store_id": store_id_to_id[store_id],
            "date": parsed_date,
            "is_active": parse_bool(row.get("is_active", "true")),
        })

    # Bulk insert
    if valid_rows:
        db.bulk_insert_mappings(PermanentJourneyPlan, valid_rows)
        db.commit()

    elapsed = round(time.time() - start_time, 3)

    return {
        "file_type": "store_user_mapping",
        "total_rows": total_rows,
        "rows_succeeded": len(valid_rows),
        "rows_failed": total_rows - len(valid_rows),
        "time_seconds": elapsed,
        "errors": all_errors,
    }


# ---------------------------------------------------------------------------
# Large file (500K) store ingestion — chunked for performance
# ---------------------------------------------------------------------------

CHUNK_SIZE = 5000


def ingest_stores_chunked(db: Session, file_content: bytes) -> dict:
    """
    Chunked ingestion for large store CSVs.
    Processes CHUNK_SIZE rows at a time to keep memory bounded.
    Uses batch get-or-create and bulk inserts.
    """
    start_time = time.time()

    text = file_content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))

    # Pre-load existing store_ids
    existing_store_ids = set(
        r[0] for r in db.execute(select(Store.store_id)).all()
    )

    # Global lookup caches (persist across chunks)
    caches = {key: {} for key in LOOKUP_MODELS}

    # Pre-load existing lookup values into caches
    for key, model_class in LOOKUP_MODELS.items():
        existing = db.execute(select(model_class)).scalars().all()
        for record in existing:
            caches[key][record.name] = record.id

    all_errors = []
    total_rows = 0
    total_succeeded = 0
    seen_store_ids = set()
    chunk = []
    chunk_start_row = 2  # Row 1 is header

    for row_num_0based, row in enumerate(reader):
        row_num = row_num_0based + 2
        total_rows += 1
        chunk.append((row_num, row))

        if len(chunk) >= CHUNK_SIZE:
            succeeded, errors = _process_store_chunk(
                db, chunk, seen_store_ids, existing_store_ids, caches
            )
            total_succeeded += succeeded
            all_errors.extend(errors)
            chunk = []

    # Process remaining rows
    if chunk:
        succeeded, errors = _process_store_chunk(
            db, chunk, seen_store_ids, existing_store_ids, caches
        )
        total_succeeded += succeeded
        all_errors.extend(errors)

    elapsed = round(time.time() - start_time, 3)

    return {
        "file_type": "stores_master_500k",
        "total_rows": total_rows,
        "rows_succeeded": total_succeeded,
        "rows_failed": total_rows - total_succeeded,
        "time_seconds": elapsed,
        "chunk_size": CHUNK_SIZE,
        "errors": all_errors,
    }


def _process_store_chunk(
    db: Session,
    chunk: list,
    seen_store_ids: set,
    existing_store_ids: set,
    caches: dict,
) -> tuple:
    """
    Process a chunk of store rows:
      1. Validate each row
      2. Batch get-or-create lookups
      3. Bulk insert valid stores
    Returns (succeeded_count, errors_list).
    """
    errors = []
    valid_rows = []

    for row_num, row in chunk:
        row_errors = validate_store_row(row, row_num, seen_store_ids, existing_store_ids)
        if row_errors:
            errors.extend(row_errors)
            continue

        store_id = _normalize(row.get("store_id", ""))
        seen_store_ids.add(store_id)

        valid_rows.append({
            "store_id": store_id,
            "store_external_id": _normalize(row.get("store_external_id", "")),
            "name": _normalize(row.get("name", "")),
            "title": _normalize(row.get("title", "")),
            "store_brand": normalize_lookup_value(row.get("store_brand", "")),
            "store_type": normalize_lookup_value(row.get("store_type", "")),
            "city": normalize_lookup_value(row.get("city", "")),
            "state": normalize_lookup_value(row.get("state", "")),
            "country": normalize_lookup_value(row.get("country", "")),
            "region": normalize_lookup_value(row.get("region", "")),
            "latitude": float(_normalize(row.get("latitude", "0")) or "0"),
            "longitude": float(_normalize(row.get("longitude", "0")) or "0"),
            "is_active": parse_bool(row.get("is_active", "true")),
        })

    # Batch resolve lookups for this chunk
    lookup_fields = ["store_brand", "store_type", "city", "state", "country", "region"]
    for field in lookup_fields:
        unique_values = {r[field] for r in valid_rows if r[field]}
        batch_get_or_create(db, LOOKUP_MODELS[field], unique_values, caches[field])

    # Build insert mappings
    store_mappings = []
    for r in valid_rows:
        store_mappings.append({
            "store_id": r["store_id"],
            "store_external_id": r["store_external_id"],
            "name": r["name"],
            "title": r["title"],
            "store_brand_id": caches["store_brand"].get(r["store_brand"]),
            "store_type_id": caches["store_type"].get(r["store_type"]),
            "city_id": caches["city"].get(r["city"]),
            "state_id": caches["state"].get(r["state"]),
            "country_id": caches["country"].get(r["country"]),
            "region_id": caches["region"].get(r["region"]),
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "is_active": r["is_active"],
        })

    if store_mappings:
        db.bulk_insert_mappings(Store, store_mappings)
        db.commit()

    return len(store_mappings), errors
