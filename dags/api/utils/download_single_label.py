from dags.api.scripts.api_client import fetch_api

transaction_id = "T123456"

fetch_api(
    endpoint=f"fraud-labels/{transaction_id}",
    filename_prefix=f"fraud_label_{transaction_id}"
)