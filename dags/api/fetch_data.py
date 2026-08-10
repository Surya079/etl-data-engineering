from airflow.decorators import task
from api.utils.api_client import fetch_api, save_json
from datetime import datetime

# Simple default page/limit – can be overridden by Airflow Variables
PAGE = 1
LIMIT = 100

@task
def transactions_data():
    return fetch_api(
        endpoint="transactions",
        params={"page": PAGE, "limit": LIMIT},
        filename_prefix=f"transactions_page_{PAGE}",
        save=False
    )

@task
def users_data():
    return fetch_api(endpoint="users", filename_prefix="users", save=False)

@task
def merchant_categories():
    return fetch_api(endpoint="merchant-categories", filename_prefix="merchant_categories", save=False)

@task
def fraud_labels():
    return fetch_api(
        endpoint="fraud-labels",
        params={"page": PAGE, "limit": LIMIT},
        filename_prefix=f"fraud_labels_page_{PAGE}",save=False
    )
@task
def combine_and_save(txn_data, usr_data, mcc_data, fraud_data):
    combined = {
        "transactions": txn_data,
        "users": usr_data,
        "merchant_categories": mcc_data,
        "fraud_labels": fraud_data,
        "metadata": {
            "page": PAGE,
            "limit": LIMIT,
            "combined_at": datetime.now().isoformat()
        }
    }
    filepath = save_json(combined, "all_data_combined")
    print(f"✅ Combined data saved to {filepath}")
    return filepath