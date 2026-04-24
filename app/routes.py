"""
API routes for CSV file upload and ingestion.

Endpoints:
  POST /api/upload/stores          — Upload stores_master.csv
  POST /api/upload/users           — Upload users_master.csv
  POST /api/upload/mapping         — Upload store_user_mapping.csv
  POST /api/upload/stores-bulk     — Upload stores_master_500k.csv (chunked)
  GET  /api/health                 — Health check
  GET  /api/stats                  — Database statistics
"""

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from app.database import get_db
from app.models import Store, User, PermanentJourneyPlan, LOOKUP_MODELS
from app.services import ingest_stores, ingest_users, ingest_mapping, ingest_stores_chunked

router = APIRouter(prefix="/api", tags=["Data Ingestion"])


@router.get("/health")
def health_check():
    return {"status": "healthy", "service": "Data Ingestion API"}


@router.get("/stats")
def database_stats(db: Session = Depends(get_db)):
    """Return row counts for all tables."""
    stats = {
        "stores": db.scalar(select(func.count()).select_from(Store)),
        "users": db.scalar(select(func.count()).select_from(User)),
        "permanent_journey_plans": db.scalar(select(func.count()).select_from(PermanentJourneyPlan)),
    }
    for key, model in LOOKUP_MODELS.items():
        stats[f"lookup_{key}"] = db.scalar(select(func.count()).select_from(model))
    return stats


@router.post("/upload/stores")
async def upload_stores(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload and ingest a stores master CSV file.
    Validates every row. Skips invalid rows and ingests valid ones.
    Returns detailed error report.
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    result = ingest_stores(db, content)
    return result


@router.post("/upload/users")
async def upload_users(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload and ingest a users master CSV file.
    Validates every row. Skips invalid rows and ingests valid ones.
    Returns detailed error report.
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    result = ingest_users(db, content)
    return result


@router.post("/upload/mapping")
async def upload_mapping(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload and ingest a store-user mapping (PJP) CSV file.
    Both stores and users must be uploaded first.
    Validates every row. Skips invalid rows and ingests valid ones.
    Returns detailed error report.
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    # Check that stores and users exist
    store_count = db.scalar(select(func.count()).select_from(Store))
    user_count = db.scalar(select(func.count()).select_from(User))
    if store_count == 0 or user_count == 0:
        raise HTTPException(
            status_code=400,
            detail=f"Upload stores and users first. Current counts — stores: {store_count}, users: {user_count}"
        )

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    result = ingest_mapping(db, content)
    return result


@router.post("/upload/stores-bulk")
async def upload_stores_bulk(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    Upload and ingest a large stores CSV file using chunked processing.
    Optimized for 500K+ rows with batch get-or-create and bulk inserts.
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    result = ingest_stores_chunked(db, content)
    return result
