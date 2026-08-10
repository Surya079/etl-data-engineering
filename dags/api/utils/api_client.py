import requests
import json
from pathlib import Path
from datetime import datetime, date
import os
import time

# Use environment variable set in docker-compose
BASE_URL = os.getenv('AIRFLOW_VAR_FASTAPI_URL', 'http://fastapi:8000')

def check_api_health():
    """Check if the API is reachable"""
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False

def fetch_api(endpoint, params=None, filename_prefix="data", max_retries=3, save=True):
    """Fetch data from API with retry logic"""
    
    # Check if API is up
    if not check_api_health():
        raise ConnectionError(
            f"❌ API at {BASE_URL} is not reachable.\n"
            "Please ensure FastAPI container is running:\n"
            "  docker-compose ps fastapi"
        )
    
    url = f"{BASE_URL}/{endpoint}"
    
    for attempt in range(max_retries):
        try:
            print(f"📡 Calling: {url} (attempt {attempt + 1}/{max_retries})")
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if save:
                file_path = save_json(data, filename_prefix)
                print(f"✅ Saved -> {file_path}")
            else:
                print(f"✅ Data fetched (not saved)")
            return data
            
        except requests.exceptions.ConnectionError as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"⏳ Connection failed. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"❌ API call failed after {max_retries} attempts")
                raise
        except requests.exceptions.RequestException as e:
            print(f"❌ API call failed: {e}")
            raise

def save_json(data, prefix):
    """Save data to JSON file with timestamp"""
    airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    folder = Path(airflow_home) / "data" / "raw"
    folder.mkdir(parents=True, exist_ok=True)
    
    timestamp = date.today()
    filename = folder / f"tran_data_{timestamp}.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    
    return str(filename)