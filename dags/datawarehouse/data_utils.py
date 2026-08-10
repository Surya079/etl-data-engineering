from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

def get_conn_cursor():
    """Get connection and cursor for the ELT database."""
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cur(conn, cur):
    """Safely close cursor and connection."""
    if cur:
        cur.close()
    if conn:
        conn.close()

def create_schema(schema):
    conn, cur = get_conn_cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn.commit()
    finally:
        close_conn_cur(conn, cur)

def create_table(schema, table_name="transaction_api"):
    """Create staging and core tables with same structure."""
    conn, cur = get_conn_cursor()
    try:
        if schema == "staging":
            # Staging table: stores raw-ish data, no transformations
            sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    transaction_id          BIGINT PRIMARY KEY,
                    transaction_date        TIMESTAMP,
                    client_id              BIGINT,
                    current_age            INT,
                    retirement_age         INT,
                    birth_year             INT,
                    birth_month            INT,
                    gender                 VARCHAR(20),
                    address                TEXT,
                    latitude               DECIMAL(10,6),
                    longitude              DECIMAL(10,6),
                    per_capita_income      DECIMAL(12,2),
                    yearly_income          DECIMAL(12,2),
                    total_debt             DECIMAL(12,2),
                    credit_score           INT,
                    num_credit_cards       INT,
                    card_id                BIGINT,
                    amount                 DECIMAL(12,2),
                    use_chip               VARCHAR(100),
                    merchant_id            BIGINT,
                    merchant_city          VARCHAR(100),
                    merchant_state         VARCHAR(20),
                    merchant_zip           VARCHAR(20),
                    mcc                    INT,
                    merchant_category      VARCHAR(255),
                    errors                 TEXT,
                    is_fraud               VARCHAR(10)
                )
            """
        else:   # core
            # Core table: final, deduplicated, enriched
            sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    transaction_id          BIGINT PRIMARY KEY,
                    transaction_date        DATE,
                    client_id              BIGINT,
                    age_group              VARCHAR(20),
                    gender                 VARCHAR(20),
                    city                   VARCHAR(100),
                    state                  VARCHAR(20),
                    per_capita_income      DECIMAL(12,2),
                    yearly_income          DECIMAL(12,2),
                    total_debt             DECIMAL(12,2),
                    credit_score           INT,
                    credit_cards_count     INT,
                    card_id                BIGINT,
                    amount                 DECIMAL(12,2),
                    transaction_type       VARCHAR(50),
                    merchant_id            BIGINT,
                    merchant_city          VARCHAR(100),
                    merchant_state         VARCHAR(20),
                    merchant_zip           VARCHAR(20),
                    mcc                    INT,
                    category               VARCHAR(255),
                    is_fraud               BOOLEAN,
                    error_flag             BOOLEAN,
                    loaded_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        cur.execute(sql)
        conn.commit()
        print(f"Table {schema}.{table_name} created/verified.")
    finally:
        close_conn_cur(conn, cur)