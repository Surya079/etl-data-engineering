from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.fetch_data import (
    transactions_data,
    users_data,
    merchant_categories,
    fraud_labels,
)

local_tz = pendulum.timezone("Europe/Malta")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
}

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",
    catchup=False,
) as dag:
    t1 = transactions_data()
    t2 = users_data()
    t3 = merchant_categories()
    t4 = fraud_labels()

    t1 >> t2 >> t3 >> t4