"""
Row-level validation for each CSV type.

Each validator returns a list of error dicts for a single row.
If the list is empty, the row is valid.

Error format: {"row": int, "column": str, "reason": str}
"""

import re
from datetime import datetime, date

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
PHONE_RE = re.compile(r"^\+?\d{7,15}$")
STORE_ID_RE = re.compile(r"^STR-\d+$")


def _normalize(value: str) -> str:
    """Strip whitespace, collapse internal spaces."""
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.strip())


def _check_required(row: dict, field: str, row_num: int, errors: list):
    """Check that a field is present and non-empty after stripping."""
    val = _normalize(row.get(field, ""))
    if not val:
        errors.append({"row": row_num, "column": field, "reason": "Required field is missing or empty"})
        return False
    return True


def _check_length(row: dict, field: str, max_len: int, row_num: int, errors: list):
    """Check that field value doesn't exceed max length."""
    val = _normalize(row.get(field, ""))
    if val and len(val) > max_len:
        errors.append({
            "row": row_num,
            "column": field,
            "reason": f"Exceeds max length of {max_len} characters (got {len(val)})"
        })
        return False
    return True


# ---------------------------------------------------------------------------
# Store Validation
# ---------------------------------------------------------------------------

def validate_store_row(row: dict, row_num: int, seen_store_ids: set, existing_store_ids: set) -> list:
    """
    Validate a single store CSV row.
    Returns a list of error dicts (empty = valid).
    """
    errors = []

    # --- Required fields ---
    has_store_id = _check_required(row, "store_id", row_num, errors)
    _check_required(row, "name", row_num, errors)
    _check_required(row, "title", row_num, errors)

    # --- Field lengths ---
    _check_length(row, "store_id", 255, row_num, errors)
    _check_length(row, "store_external_id", 255, row_num, errors)
    _check_length(row, "name", 255, row_num, errors)
    _check_length(row, "title", 255, row_num, errors)
    _check_length(row, "store_brand", 255, row_num, errors)
    _check_length(row, "store_type", 255, row_num, errors)
    _check_length(row, "city", 255, row_num, errors)
    _check_length(row, "state", 255, row_num, errors)
    _check_length(row, "country", 255, row_num, errors)
    _check_length(row, "region", 255, row_num, errors)

    # --- Store ID format ---
    store_id = _normalize(row.get("store_id", ""))
    if has_store_id and store_id and not STORE_ID_RE.match(store_id):
        errors.append({
            "row": row_num,
            "column": "store_id",
            "reason": f"Invalid store_id format '{store_id}' — expected STR-XXXX"
        })

    # --- Uniqueness (within file) ---
    if has_store_id and store_id:
        if store_id in seen_store_ids:
            errors.append({
                "row": row_num,
                "column": "store_id",
                "reason": f"Duplicate store_id '{store_id}' within the file"
            })
        elif store_id in existing_store_ids:
            errors.append({
                "row": row_num,
                "column": "store_id",
                "reason": f"store_id '{store_id}' already exists in the database"
            })

    # --- Latitude ---
    lat_str = _normalize(row.get("latitude", ""))
    if lat_str:
        try:
            lat = float(lat_str)
            if lat < -90 or lat > 90:
                errors.append({
                    "row": row_num,
                    "column": "latitude",
                    "reason": f"Latitude {lat} out of range [-90, 90]"
                })
        except (ValueError, TypeError):
            errors.append({
                "row": row_num,
                "column": "latitude",
                "reason": f"Invalid latitude value '{lat_str}' — must be a number"
            })

    # --- Longitude ---
    lon_str = _normalize(row.get("longitude", ""))
    if lon_str:
        try:
            lon = float(lon_str)
            if lon < -180 or lon > 180:
                errors.append({
                    "row": row_num,
                    "column": "longitude",
                    "reason": f"Longitude {lon} out of range [-180, 180]"
                })
        except (ValueError, TypeError):
            errors.append({
                "row": row_num,
                "column": "longitude",
                "reason": f"Invalid longitude value '{lon_str}' — must be a number"
            })

    return errors


# ---------------------------------------------------------------------------
# User Validation
# ---------------------------------------------------------------------------

def validate_user_row(row: dict, row_num: int, seen_usernames: set,
                      existing_usernames: set, all_usernames_in_file: set) -> list:
    """
    Validate a single user CSV row.
    Returns a list of error dicts (empty = valid).
    """
    errors = []

    # --- Required fields ---
    has_username = _check_required(row, "username", row_num, errors)
    _check_required(row, "email", row_num, errors)

    # --- Field lengths ---
    _check_length(row, "username", 150, row_num, errors)
    _check_length(row, "first_name", 150, row_num, errors)
    _check_length(row, "last_name", 150, row_num, errors)
    _check_length(row, "email", 254, row_num, errors)
    _check_length(row, "phone_number", 32, row_num, errors)

    # --- Username uniqueness ---
    username = _normalize(row.get("username", ""))
    if has_username and username:
        if username in seen_usernames:
            errors.append({
                "row": row_num,
                "column": "username",
                "reason": f"Duplicate username '{username}' within the file"
            })
        elif username in existing_usernames:
            errors.append({
                "row": row_num,
                "column": "username",
                "reason": f"Username '{username}' already exists in the database"
            })

    # --- Email format ---
    email = _normalize(row.get("email", ""))
    if email and not EMAIL_RE.match(email):
        errors.append({
            "row": row_num,
            "column": "email",
            "reason": f"Invalid email format '{email}'"
        })

    # --- User type ---
    user_type_str = _normalize(row.get("user_type", ""))
    if user_type_str:
        try:
            ut = int(user_type_str)
            if ut not in (1, 2, 3, 7):
                errors.append({
                    "row": row_num,
                    "column": "user_type",
                    "reason": f"Invalid user_type {ut} — must be one of [1, 2, 3, 7]"
                })
        except (ValueError, TypeError):
            errors.append({
                "row": row_num,
                "column": "user_type",
                "reason": f"Invalid user_type '{user_type_str}' — must be an integer"
            })

    # --- Phone number format ---
    phone = _normalize(row.get("phone_number", ""))
    if phone and not PHONE_RE.match(phone):
        errors.append({
            "row": row_num,
            "column": "phone_number",
            "reason": f"Invalid phone number format '{phone}' — expected digits with optional + prefix"
        })

    # --- Supervisor reference ---
    supervisor = _normalize(row.get("supervisor_username", ""))
    if supervisor:
        # Supervisor must exist either in the current file (already validated rows) or in DB
        if supervisor not in all_usernames_in_file and supervisor not in existing_usernames:
            errors.append({
                "row": row_num,
                "column": "supervisor_username",
                "reason": f"Supervisor '{supervisor}' does not exist in file or database"
            })

    return errors


# ---------------------------------------------------------------------------
# Store-User Mapping (PJP) Validation
# ---------------------------------------------------------------------------

def validate_mapping_row(row: dict, row_num: int,
                         existing_usernames: set, existing_store_ids: set,
                         seen_combinations: set) -> list:
    """
    Validate a single PJP mapping CSV row.
    Returns a list of error dicts (empty = valid).
    """
    errors = []

    # --- Required fields ---
    has_username = _check_required(row, "username", row_num, errors)
    has_store_id = _check_required(row, "store_id", row_num, errors)
    has_date = _check_required(row, "date", row_num, errors)

    username = _normalize(row.get("username", ""))
    store_id = _normalize(row.get("store_id", ""))
    date_str = _normalize(row.get("date", ""))

    # --- Foreign key: username must exist in users table ---
    if has_username and username and username not in existing_usernames:
        errors.append({
            "row": row_num,
            "column": "username",
            "reason": f"User '{username}' does not exist in the database"
        })

    # --- Foreign key: store_id must exist in stores table ---
    if has_store_id and store_id and store_id not in existing_store_ids:
        errors.append({
            "row": row_num,
            "column": "store_id",
            "reason": f"Store '{store_id}' does not exist in the database"
        })

    # --- Date format and validity ---
    parsed_date = None
    if has_date and date_str:
        try:
            parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            # Sanity check: date should be within a reasonable range
            if parsed_date.year < 2000 or parsed_date.year > 2050:
                errors.append({
                    "row": row_num,
                    "column": "date",
                    "reason": f"Date '{date_str}' is outside reasonable range (2000-2050)"
                })
        except ValueError:
            errors.append({
                "row": row_num,
                "column": "date",
                "reason": f"Invalid date format '{date_str}' — expected YYYY-MM-DD"
            })

    # --- is_active must be boolean ---
    is_active_str = _normalize(row.get("is_active", "")).lower()
    if is_active_str and is_active_str not in ("true", "false", "1", "0", "yes", "no"):
        errors.append({
            "row": row_num,
            "column": "is_active",
            "reason": f"Invalid is_active value '{row.get('is_active', '')}' — expected True/False"
        })

    # --- Uniqueness: (username, store_id, date) ---
    if has_username and has_store_id and has_date and parsed_date is not None:
        combo = (username, store_id, date_str)
        if combo in seen_combinations:
            errors.append({
                "row": row_num,
                "column": "username+store_id+date",
                "reason": f"Duplicate mapping ({username}, {store_id}, {date_str}) within the file"
            })

    return errors
