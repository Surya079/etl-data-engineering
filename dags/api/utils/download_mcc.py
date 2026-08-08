from dags.api.scripts.api_client import fetch_api

fetch_api(
    endpoint="merchant-categories",
    filename_prefix="merchant_categories"
)