from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import uvicorn

from services import (
    get_transactions,
    get_users,
    get_mcc,
    get_fraud_labels,
    get_fraud_label_by_id
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Bank Transaction API...")
    yield
    logger.info("Shutting down Bank Transaction API...")

app = FastAPI(
    title="Bank Transaction API",
    description="API for accessing bank transaction data",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "UP"}

@app.get("/transactions")
def transactions(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(100, ge=1, le=1000, description="Items per page (max 1000)")
):
    try:
        return get_transactions(page, limit)
    except Exception as e:
        logger.error(f"Error fetching transactions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users")
def users():
    try:
        return get_users()
    except Exception as e:
        logger.error(f"Error fetching users: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/merchant-categories")
def merchant_categories():
    try:
        return get_mcc()
    except Exception as e:
        logger.error(f"Error fetching merchant categories: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fraud-labels")
def fraud_labels(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(100, ge=1, le=1000, description="Items per page (max 1000)")
):
    """
    Get paginated fraud labels
    Structure: {"target": {"transaction_id": "Yes/No", ...}}
    """
    try:
        return get_fraud_labels(page=page, limit=limit)
    except Exception as e:
        logger.error(f"Error fetching fraud labels: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fraud-labels/{transaction_id}")
def fraud_label_by_id(transaction_id: str):
    """
    Look up fraud label for a specific transaction ID
    Much faster than loading all labels
    """
    try:
        result = get_fraud_label_by_id(transaction_id)
        if not result.get("found"):
            raise HTTPException(status_code=404, detail="Transaction ID not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error looking up fraud label: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fraud-labels/info")
def fraud_labels_info():
    """Get information about fraud labels file without loading it"""
    from pathlib import Path
    import os
    
    file_path = Path(__file__).parent / "data" / "train_fraud_labels.json"
    if file_path.exists():
        size_mb = os.path.getsize(file_path) / (1024**2)
        return {
            "file_exists": True,
            "file_size_mb": round(size_mb, 2),
            "file_path": str(file_path)
        }
    return {"file_exists": False}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)