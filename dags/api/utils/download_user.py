from dags.api.scripts.api_client import fetch_api

fetch_api(
    endpoint="users",
    filename_prefix="users"
)