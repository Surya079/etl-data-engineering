"""
Integration tests for the ELT pipeline.

These tests require a PostgreSQL test database. The connection details
are provided via environment variables (TEST_DB_HOST, etc.) which are
set in the CI workflow. They create the schemas/tables, load sample data,
run transformations, and validate the data in the database.
"""

import pytest
import os
import json
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

# Import functions under test
from dags.datawarehouse.data_utils import create_schema, create_table, get_conn_cursor, close_conn_cur
from dags.datawarehouse.dwh import load_staging_from_file, transform_to_core
from dags.datawarehouse.data_modification import load_rows_to_staging, load_rows_to_core
from dags.datawarehouse.data_transformation import transform_transactions


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------
@pytest.fixture(scope='session')
def db_connection():
    """Create a connection to the test database using env vars."""
    conn = psycopg2.connect(
        host=os.getenv('TEST_DB_HOST', 'localhost'),
        port=os.getenv('TEST_DB_PORT', '5432'),
        dbname=os.getenv('TEST_DB_NAME', 'test_db'),
        user=os.getenv('TEST_DB_USER', 'test_user'),
        password=os.getenv('TEST_DB_PASSWORD', 'test_pass')
    )
    yield conn
    conn.close()


@pytest.fixture(scope='function')
def setup_database(db_connection):
    """
    Create schemas and tables before each test, and clean them up afterward.
    This ensures a known starting state for every test.
    """
    cur = db_connection.cursor()
    # Clean up from previous runs (if any)
    cur.execute("DROP TABLE IF EXISTS core.transaction_api CASCADE;")
    cur.execute("DROP TABLE IF EXISTS staging.transaction_api CASCADE;")
    db_connection.commit()
    cur.close()

    # Create schemas and tables using project functions
    create_schema('staging')
    create_table('staging')
    create_schema('core')
    create_table('core')
    yield
    # Teardown: drop tables
    cur = db_connection.cursor()
    cur.execute("DROP TABLE IF EXISTS staging.transaction_api CASCADE;")
    cur.execute("DROP TABLE IF EXISTS core.transaction_api CASCADE;")
    db_connection.commit()
    cur.close()


@pytest.fixture
def sample_data_file(tmp_path):
    """Create a temporary sample combined JSON file for testing."""
    # Use the sample file from the repo if available; otherwise create a minimal one.
    repo_sample = Path(__file__).parent / 'sample_combined.json'
    if repo_sample.exists():
        return str(repo_sample)
    # Create a temporary sample file for demonstration
    sample = {
        "transactions": {"data": [
            {"id": "1001", "date": "2023-01-01 10:00:00", "client_id": "501",
             "card_id": "101", "amount": "$50.00", "use_chip": "Swipe Transaction",
             "merchant_id": "2001", "merchant_city": "Denver", "merchant_state": "CO",
             "zip": "80202.0", "mcc": "5812", "errors": None},
            {"id": "1002", "date": "2023-01-02 11:30:00", "client_id": "502",
             "card_id": "102", "amount": "$120.75", "use_chip": "Chip Transaction",
             "merchant_id": "2002", "merchant_city": "", "merchant_state": "",
             "zip": "10001.0", "mcc": "9999", "errors": "missing city"},
        ]},
        "users": [
            {"id": 501, "current_age": 28, "gender": "Male", "per_capita_income": "$35000",
             "yearly_income": "$70000", "total_debt": "$15000", "credit_score": 750,
             "num_credit_cards": 2},
            {"id": 502, "current_age": 45, "gender": "Female", "per_capita_income": "$50000",
             "yearly_income": "$90000", "total_debt": "$30000", "credit_score": 800,
             "num_credit_cards": 4},
        ],
        "merchant_categories": {"5812": "Eating Places"},
        "fraud_labels": {"data": [
            {"transaction_id": "1001", "is_fraud": "No"},
            {"transaction_id": "1002", "is_fraud": "Yes"},
        ]},
        "metadata": {}
    }
    file_path = tmp_path / 'sample_combined.json'
    with open(file_path, 'w') as f:
        json.dump(sample, f)
    return str(file_path)


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------
class TestStagingLoad:
    def test_load_staging_from_file_inserts_rows(self, setup_database, sample_data_file):
        """Verify that staging rows are inserted from a combined JSON file."""
        load_staging_from_file(sample_data_file)

        conn, cur = get_conn_cursor()
        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM staging.transaction_api")
            count = cur.fetchone()['cnt']
        finally:
            close_conn_cur(conn, cur)

        assert count == 2   # two transactions in sample

    def test_load_staging_handles_duplicates(self, setup_database, sample_data_file):
        """Ensure upsert works: loading same file twice doesn't duplicate rows."""
        load_staging_from_file(sample_data_file)
        load_staging_from_file(sample_data_file)   # second load

        conn, cur = get_conn_cursor()
        try:
            cur.execute("SELECT COUNT(*) AS cnt FROM staging.transaction_api")
            count = cur.fetchone()['cnt']
        finally:
            close_conn_cur(conn, cur)

        assert count == 2   # no duplicates because ON CONFLICT DO NOTHING


class TestCoreTransformation:
    def test_transform_to_core_populates_no_nulls(self, setup_database, sample_data_file):
        """After transformation, critical columns in core should not contain NULLs."""
        load_staging_from_file(sample_data_file)
        transform_to_core()

        conn, cur = get_conn_cursor()
        try:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE age_group IS NULL) AS null_age_group,
                    COUNT(*) FILTER (WHERE transaction_type IS NULL) AS null_txn_type,
                    COUNT(*) FILTER (WHERE is_fraud IS NULL) AS null_fraud,
                    COUNT(*) FILTER (WHERE credit_score IS NULL) AS null_credit
                FROM core.transaction_api
            """)
            result = cur.fetchone()
        finally:
            close_conn_cur(conn, cur)

        assert result['null_age_group'] == 0
        assert result['null_txn_type'] == 0
        assert result['null_fraud'] == 0
        assert result['null_credit'] == 0

    def test_core_table_schema(self, setup_database):
        """Validate core table schema (column names and types)."""
        conn, cur = get_conn_cursor()
        try:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'core' AND table_name = 'transaction_api'
                ORDER BY ordinal_position
            """)
            columns = {row['column_name']: row['data_type'] for row in cur.fetchall()}
        finally:
            close_conn_cur(conn, cur)

        assert columns['transaction_id'] == 'bigint'
        assert columns['transaction_date'] == 'date'
        assert columns['amount'] == 'numeric'
        assert columns['is_fraud'] == 'boolean'
        assert columns['age_group'] == 'character varying'  # or 'text'