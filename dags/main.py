import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dataquality.soda import tran_elt_data_quality

from api.fetch_data import (
    transactions_data,
    users_data,
    merchant_categories,
    fraud_labels,
    combine_and_save,
)
from datawarehouse.dwh import load_staging_from_file, transform_to_core

local_tz = pendulum.timezone("Europe/Malta")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
}

# ============================================================
# DAG 1: Extract data from APIs and save as combined JSON
# ============================================================
with DAG(
    dag_id="extract_and_save",
    default_args=default_args,
    description="Extract banking data from APIs and save as combined JSON",
    schedule="0 14 * * *",
    catchup=False,
) as extract_dag:

    t1 = transactions_data()
    t2 = users_data()
    t3 = merchant_categories()
    t4 = fraud_labels()

    combined_file = combine_and_save(t1, t2, t3, t4)

    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="load_to_staging",
        conf={
            "file_path": "{{ task_instance.xcom_pull(task_ids='combine_and_save') }}"
        },
        wait_for_completion=False,
    )

    [t1, t2, t3, t4] >> combined_file >> trigger_load


# ============================================================
# DAG 2: Load combined JSON into staging table
# ============================================================
with DAG(
    dag_id="load_to_staging",
    default_args=default_args,
    description="Load combined JSON file into staging.transaction_api",
    schedule=None,
    catchup=False,
) as load_dag:

    def _load_staging(**context):
        dag_run = context["dag_run"]
        file_path = dag_run.conf.get("file_path")
        if not file_path:
            raise ValueError("No file_path provided in dag_run conf")
        load_staging_from_file(file_path)
        return file_path

    load_task = PythonOperator(
        task_id="load_staging_task",
        python_callable=_load_staging,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="transform_to_core",
        wait_for_completion=False,
    )

    load_task >> trigger_transform


# ============================================================
# DAG 3: Transform staging data into core table
# ============================================================
with DAG(
    dag_id="transform_to_core",
    default_args=default_args,
    description="Transform staging data and upsert into core.transaction_api",
    schedule=None,
    catchup=False,
) as transform_dag:

    transform_task = PythonOperator(
        task_id="transform_core_task",
        python_callable=transform_to_core,
    )

    # Trigger the data quality DAG after transformation succeeds
    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
        wait_for_completion=False,
    )

    transform_task >> trigger_quality


# ============================================================
# DAG 4: Data quality checks (triggered after transform)
# ============================================================
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="Run Soda data quality checks on staging and core",
    schedule=None,
    catchup=False,
) as quality_dag:

    check_staging = PythonOperator(
        task_id="check_staging_quality",
        python_callable=tran_elt_data_quality,
        op_kwargs={"schema": "staging"},
    )

    check_core = PythonOperator(
        task_id="check_core_quality",
        python_callable=tran_elt_data_quality,
        op_kwargs={"schema": "core"},
    )

    check_staging >> check_core