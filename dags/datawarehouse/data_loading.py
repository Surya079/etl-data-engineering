import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def load_combined_data(file_path):
    """Load the combined JSON file."""
    try:
        logger.info(f"Loading combined data from {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise