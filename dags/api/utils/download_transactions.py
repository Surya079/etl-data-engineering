import random
from dags.api.scripts.api_client import fetch_api

LIMIT = 5
TOTAL_RECORDS = 13305915

TOTAL_PAGES = (TOTAL_RECORDS // LIMIT) + 1

page = random.randint(1, TOTAL_PAGES)

fetch_api(
    endpoint="transactions",
    params={
        "page": page,
        "limit": LIMIT
    },
    filename_prefix=f"transactions_page_{page}"
)