# Retail Data Ingestion API

A robust FastAPI backend service designed to ingest, validate, and normalize large master data sets (stores, users, and mapping data).

## Features
- **FastAPI / SQLAlchemy** using SQLite (swappable to PostgreSQL)
- **Comprehensive Validation**: format checks, constraints, lengths, and foreign key dependencies.
- **Fail-Resilient Processing**: Skips invalid rows and returns a detailed JSON error report instead of aborting huge ETL payloads.
- **Performance Optimized**: Features sequential chunking and `bulk_insert_mappings()` over row-by-row iteration for ingestion of 500K+ records.

## Setup & Run

```bash
# 1. Install Dependencies
pip install -r requirements.txt

# 2. Run the Server
uvicorn app.main:app --reload
```

## Performance Evidence

Ingesting a **500,000 row CSV** (`stores_master_500k.csv`):

```text
=======================================================
  500K STORE INGESTION — PERFORMANCE REPORT
=======================================================
  Total rows in file  : 500,000
  Rows succeeded      : 491,449
  Rows failed         : 8,551
  Chunk size          : 5,000
  Wall-clock time     : 268.73s (~4.5 mins)
  Throughput          : 1,861 rows/sec

  Sample errors (first 5):
    Row 49 | store_id | Duplicate store_id 'STR-0000026' within the file
    Row 83 | title | Required field is missing or empty
    Row 84 | latitude | Invalid latitude value 'INVALID' — must be a number
    Row 283 | name | Exceeds max length of 255 characters (got 300)
    Row 335 | latitude | Invalid latitude value 'INVALID' — must be a number
=======================================================
```
