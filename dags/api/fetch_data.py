from airflow.decorators import task
from airflow.models import Variable
from api.utils.api_client import fetch_api

# Simple default page/limit – can be overridden by Airflow Variables
PAGE = 1
LIMIT = 100

@task
def transactions_data():
    fetch_api(
        endpoint="transactions",
        params={"page": PAGE, "limit": LIMIT},
        filename_prefix=f"transactions_page_{PAGE}",
    )

@task
def users_data():
    fetch_api(endpoint="users", filename_prefix="users")

@task
def merchant_categories():
    fetch_api(endpoint="merchant-categories", filename_prefix="merchant_categories")

@task
def fraud_labels():
    fetch_api(
        endpoint="fraud-labels",
        params={"page": PAGE, "limit": LIMIT},
        filename_prefix=f"fraud_labels_page_{PAGE}",
    )