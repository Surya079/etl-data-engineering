import random
from dags.api.scripts.api_client import fetch_api

LIMIT = 100
TOTAL_RECORDS = 13305915

TOTAL_PAGES = (TOTAL_RECORDS // LIMIT) + 1

page = random.randint(1, TOTAL_PAGES)

fetch_api(
    endpoint="fraud-labels",
    params={
        "page": page,
        "limit": LIMIT
    },
    filename_prefix=f"fraud_labels_page_{page}"
)