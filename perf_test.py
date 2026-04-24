"""
Standalone performance test for 500K store ingestion.
Bypasses HTTP and calls the service layer directly so we get accurate timing.
Resets the stores table first so numbers are clean.
"""

import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import engine, Base, SessionLocal
from app.models import Store, StoreBrand, StoreType, City, State, Country, Region
from app.services import ingest_stores_chunked

# ---- Reset stores (keep lookup tables to avoid re-creating them) ----
with engine.connect() as conn:
    conn.execute(Store.__table__.delete())
    conn.commit()

db = SessionLocal()
DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "stores_master_500k.csv")

with open(DATA_FILE, "rb") as f:
    content = f.read()

file_size_mb = len(content) / (1024 * 1024)
print(f"File size: {file_size_mb:.1f} MB")
print("Starting ingestion...")

start = time.time()
result = ingest_stores_chunked(db, content)
elapsed = time.time() - start

db.close()

# ---- Report ----
print("\n" + "="*55)
print("  500K STORE INGESTION — PERFORMANCE REPORT")
print("="*55)
print(f"  Total rows in file  : {result['total_rows']:,}")
print(f"  Rows succeeded      : {result['rows_succeeded']:,}")
print(f"  Rows failed         : {result['rows_failed']:,}")
print(f"  Chunk size          : {result['chunk_size']:,}")
print(f"  Wall-clock time     : {elapsed:.2f}s")
print(f"  Throughput          : {result['total_rows']/elapsed:,.0f} rows/sec")
print(f"  Errors in this run  : {len(result['errors'])}")
if result['errors']:
    print("\n  Sample errors (first 5):")
    for e in result['errors'][:5]:
        print(f"    Row {e['row']} | {e['column']} | {e['reason']}")
print("="*55)
