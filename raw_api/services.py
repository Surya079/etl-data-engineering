from pathlib import Path
import pandas as pd
import json
from functools import lru_cache
import logging
import numpy as np
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

# Cache only small data, not large files
@lru_cache(maxsize=1)
def load_users_df():
    """Load users data (assuming it's smaller)"""
    try:
        file_path = DATA_DIR / "users_data.csv"
        logger.info(f"Loading users from {file_path}")
        df = pd.read_csv(file_path)
        df = df.replace([np.inf, -np.inf], np.nan)
        return df
    except Exception as e:
        logger.error(f"Error loading users: {e}")
        raise

@lru_cache(maxsize=1)
def load_mcc_codes():
    """Load MCC codes"""
    try:
        file_path = DATA_DIR / "mcc_codes.json"
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading MCC codes: {e}")
        raise

def get_file_size_mb(file_path):
    """Get file size in MB"""
    if not file_path.exists():
        return 0
    return os.path.getsize(file_path) / (1024**2)

def count_lines_fast(file_path):
    """Count lines in a file without reading it all into memory"""
    count = 0
    with open(file_path, 'rb') as f:
        while chunk := f.read(1024 * 1024):
            count += chunk.count(b'\n')
    return count

def get_transactions(page: int = 1, limit: int = 100):
    """Get paginated transactions - memory efficient for large files"""
    try:
        page = max(1, int(page))
        limit = max(1, min(1000, int(limit)))
        
        file_path = DATA_DIR / "transactions_data.csv"
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_size = get_file_size_mb(file_path)
        logger.info(f"Transactions file size: {file_size:.2f} MB")
        
        total_rows = count_lines_fast(file_path) - 1
        logger.info(f"Total transactions: {total_rows}")
        
        start_row = (page - 1) * limit + 1
        end_row = start_row + limit - 1
        
        if start_row > total_rows:
            return {
                "page": page,
                "limit": limit,
                "total": total_rows,
                "count": 0,
                "data": []
            }
        
        logger.info(f"Reading rows {start_row} to {min(end_row, total_rows)}")
        
        header = pd.read_csv(file_path, nrows=0).columns.tolist()
        
        df = pd.read_csv(
            file_path,
            skiprows=range(1, start_row),
            nrows=limit,
            names=header,
            header=None
        )
        
        df = clean_dataframe(df)
        
        records = json.loads(
            df.to_json(orient="records", date_format="iso")
        )
        
        return {
            "page": page,
            "limit": limit,
            "total": total_rows,
            "count": len(records),
            "data": records[1:]
        }
        
    except Exception as e:
        logger.error(f"Error in get_transactions: {e}", exc_info=True)
        raise

def clean_dataframe(df):
    """Replace NaN and infinite values with None for JSON serialization"""
    return df.where(pd.notnull(df), None).replace([np.inf, -np.inf], None)

def get_users():
    """Get all users"""
    try:
        df = load_users_df()
        df = clean_dataframe(df)
        return json.loads(
            df.to_json(orient="records", date_format="iso")
        )
    except Exception as e:
        logger.error(f"Error in get_users: {e}", exc_info=True)
        raise

def get_mcc():
    """Get MCC codes"""
    return load_mcc_codes()

def get_fraud_labels(transaction_id: str = None, page: int = 1, limit: int = 100):
    """
    Get fraud labels - OPTIMIZED for structure: {"target": {"id1": "Yes/No", ...}}
    
    Args:
        transaction_id: Specific transaction ID to look up (optional)
        page: Page number for pagination
        limit: Number of records per page
    """
    file_path = DATA_DIR / "train_fraud_labels.json"
    
    if not file_path.exists():
        logger.error(f"Fraud labels file not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_size_mb = get_file_size_mb(file_path)
    logger.info(f"Fraud labels file size: {file_size_mb:.2f} MB")
    
    try:
        # If looking for specific transaction ID
        if transaction_id:
            return lookup_fraud_label(file_path, transaction_id)
        
        # Otherwise return paginated results
        return stream_fraud_labels(file_path, page, limit)
        
    except MemoryError:
        logger.error("Memory error loading fraud labels")
        return {
            "error": "File too large to load into memory",
            "file_size_mb": round(file_size_mb, 2),
            "suggestion": "Use /fraud-labels/{transaction_id} endpoint for specific lookups"
        }
    except Exception as e:
        logger.error(f"Error loading fraud labels: {e}", exc_info=True)
        raise

def lookup_fraud_label(file_path, transaction_id):
    """
    Stream through the JSON file to find a specific transaction ID
    Much faster than loading entire file for single lookup
    """
    import re
    
    logger.info(f"Looking up fraud label for transaction: {transaction_id}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        # Skip the 's' prefix if present
        first_char = f.read(1)
        if first_char != '{' and first_char != 's':
            f.seek(0)
        elif first_char == 's':
            # Already skipped 's', continue
            pass
        else:
            f.seek(0)
        
        # Read the file in chunks and search for the transaction ID
        chunk_size = 1024 * 1024  # 1MB chunks
        pattern = f'"{transaction_id}": "([^"]*)"'.encode()
        
        buffer = b""
        while True:
            chunk = f.read(chunk_size).encode() if hasattr(f.read(chunk_size), 'encode') else f.read(chunk_size)
            if not chunk:
                break
            
            if isinstance(chunk, str):
                chunk = chunk.encode()
            
            buffer += chunk
            
            # Search for the transaction ID pattern
            match = re.search(pattern, buffer)
            if match:
                value = match.group(1).decode()
                return {
                    "transaction_id": transaction_id,
                    "is_fraud": value,
                    "found": True
                }
            
            # Keep last part of buffer that might contain partial match
            if len(buffer) > 1000:
                buffer = buffer[-1000:]
    
    return {
        "transaction_id": transaction_id,
        "found": False,
        "message": "Transaction ID not found in fraud labels"
    }

def stream_fraud_labels(file_path, page=1, limit=100):
    """
    Stream through the JSON file and extract fraud labels in chunks
    Specifically designed for structure: {"target": {"id1": "Yes/No", ...}}
    """
    import re
    
    page = max(1, int(page))
    limit = max(1, min(1000, int(limit)))
    
    logger.info(f"Streaming fraud labels - page {page}, limit {limit}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        # Read the entire file (but process it as text, not parsed JSON)
        content = f.read()
    
    # Remove the 's' prefix if present
    if content.startswith('s'):
        content = content[1:]
    
    # Extract the target dictionary using regex
    # This is much more memory efficient than json.loads()
    target_match = re.search(r'"target"\s*:\s*\{', content)
    if not target_match:
        return {"error": "Could not find 'target' key in fraud labels"}
    
    # Find the start of the target object
    start_pos = target_match.end()
    
    # Extract key-value pairs using regex iterator
    # Pattern: "id": "value"
    pattern = re.compile(r'"(\d+)":\s*"(Yes|No)"')
    
    all_labels = []
    total_count = 0
    
    # Process the string from start_pos onwards
    remaining = content[start_pos:]
    
    for match in pattern.finditer(remaining):
        total_count += 1
        
        # Calculate which page this item belongs to
        item_page = ((total_count - 1) // limit) + 1
        
        if item_page == page:
            all_labels.append({
                "transaction_id": match.group(1),
                "is_fraud": match.group(2)
            })
        
        # Stop if we've collected enough for this page
        if len(all_labels) >= limit and item_page > page:
            break
    
    # Calculate pagination
    total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
    
    return {
        "page": page,
        "limit": limit,
        "total": total_count,
        "total_pages": total_pages,
        "count": len(all_labels),
        "data": all_labels
    }

def get_fraud_labels_paginated(page: int = 1, limit: int = 100):
    """Get paginated fraud labels"""
    return stream_fraud_labels(DATA_DIR / "train_fraud_labels.json", page, limit)

def get_fraud_label_by_id(transaction_id: str):
    """Look up fraud label for specific transaction ID"""
    return lookup_fraud_label(DATA_DIR / "train_fraud_labels.json", transaction_id)