"""
FastAPI application entry point.

Starts the Data Ingestion Service, creates all DB tables on startup,
and mounts the API router.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import engine, Base
from app.routes import router

# Create all tables on startup
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Data Ingestion Service",
    description=(
        "Backend API for a retail platform. "
        "Upload CSV files (stores, users, store-user mapping), "
        "validate every row, and ingest into a SQL database."
    ),
    version="1.0.0",
)

# CORS (allow Postman / any frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routes
app.include_router(router)


@app.get("/")
def root():
    return {
        "service": "Data Ingestion Service",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "upload_stores": "POST /api/upload/stores",
            "upload_users": "POST /api/upload/users",
            "upload_mapping": "POST /api/upload/mapping",
            "upload_stores_bulk": "POST /api/upload/stores-bulk",
            "health": "GET /api/health",
            "stats": "GET /api/stats",
        },
    }
