import logging
import subprocess
logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"


def tran_elt_data_quality(schema):
    """
    Run Soda scan for the given schema (staging / core).
    Returns the stdout from the scan.
    Raises an exception if checks fail.
    """
    config = "/opt/airflow/include/soda/configuration.yml"
    checks = f"/opt/airflow/include/soda/checks.yml"

    cmd = [
        "soda", "scan",
        "-d", "pg_datasource",
        "-c", config,
        checks,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    print("STDOUT:", result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise Exception(f"Soda scan FAILED for schema '{schema}'.\n{result.stderr}")

    return result.stdout   # plain string, not a task object