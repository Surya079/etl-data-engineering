import logging
from psycopg2.extras import execute_values
from .data_utils import get_conn_cursor, close_conn_cur

logger = logging.getLogger(__name__)

STAGING_TABLE = "staging.transaction_api"
CORE_TABLE = "core.transaction_api"

def load_rows_to_staging(staging_rows):
    """Insert all staging rows in one batch using UPSERT."""
    if not staging_rows:
        return
    conn, cur = get_conn_cursor()
    try:
        # Build a list of tuples for each row (ordered by columns)
        columns = [
            "transaction_id", "transaction_date", "client_id", "current_age",
            "retirement_age", "birth_year", "birth_month", "gender", "address",
            "latitude", "longitude", "per_capita_income", "yearly_income",
            "total_debt", "credit_score", "num_credit_cards", "card_id",
            "amount", "use_chip", "merchant_id", "merchant_city",
            "merchant_state", "merchant_zip", "mcc", "merchant_category",
            "errors", "is_fraud"
        ]
        tuples = [[row[col] for col in columns] for row in staging_rows]
        
        execute_values(cur, f"""
            INSERT INTO {STAGING_TABLE} ({", ".join(columns)})
            VALUES %s
            ON CONFLICT (transaction_id) DO UPDATE SET
                transaction_date = EXCLUDED.transaction_date,
                amount = EXCLUDED.amount,
                merchant_category = EXCLUDED.merchant_category,
                is_fraud = EXCLUDED.is_fraud
        """, tuples)
        conn.commit()
        logger.info(f"Inserted/Updated {len(staging_rows)} rows into staging.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Staging load failed: {e}")
        raise
    finally:
        close_conn_cur(conn, cur)

def load_rows_to_core(core_rows):
    """Insert all core rows in one batch with UPSERT."""
    if not core_rows:
        return
    conn, cur = get_conn_cursor()
    try:
        columns = [
            "transaction_id", "transaction_date", "client_id", "age_group",
            "gender", "city", "state", "per_capita_income", "yearly_income",
            "total_debt", "credit_score", "credit_cards_count", "card_id",
            "amount", "transaction_type", "merchant_id", "merchant_city",
            "merchant_state", "merchant_zip", "mcc", "category", "is_fraud",
            "error_flag"
        ]
        tuples = [[row[col] for col in columns] for row in core_rows]
        
        execute_values(cur, f"""
            INSERT INTO {CORE_TABLE} ({", ".join(columns)})
            VALUES %s
            ON CONFLICT (transaction_id) DO UPDATE SET
                amount = EXCLUDED.amount,
                category = EXCLUDED.category,
                is_fraud = EXCLUDED.is_fraud,
                error_flag = EXCLUDED.error_flag,
                loaded_at = CURRENT_TIMESTAMP
        """, tuples)
        conn.commit()
        logger.info(f"Inserted/Updated {len(core_rows)} rows into core.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Core load failed: {e}")
        raise
    finally:
        close_conn_cur(conn, cur)