import logging
from datawarehouse.data_utils import create_schema, create_table, get_conn_cursor, close_conn_cur
from datawarehouse.data_loading import load_combined_data
from datawarehouse.data_transformation import transform_transactions, transform_staging_to_core
from datawarehouse.data_modification import load_rows_to_staging, load_rows_to_core

logger = logging.getLogger(__name__)

def load_staging_from_file(file_path):
    """Read combined JSON and load into staging."""
    create_schema("staging")
    create_table("staging")
    raw_data = load_combined_data(file_path)
    # transform_transactions returns list of (staging_row, core_row)
    transformed = transform_transactions(raw_data)
    staging_rows = [s for s, _ in transformed]
    load_rows_to_staging(staging_rows)
    logger.info(f"Loaded {len(staging_rows)} rows to staging.")

def transform_to_core():
    """Read all rows from staging, transform, and upsert into core."""
    create_schema("core")
    create_table("core")
    conn, cur = get_conn_cursor()
    try:
        cur.execute("SELECT * FROM staging.transaction_api")
        staging_rows = cur.fetchall()
    finally:
        close_conn_cur(conn, cur)

    # Transform each staging row into a core row
    core_rows = [transform_staging_to_core(row) for row in staging_rows]
    load_rows_to_core(core_rows)
    logger.info(f"Upserted {len(core_rows)} rows into core.")