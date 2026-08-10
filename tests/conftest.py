import pytest
from airflow.models import Connection

@pytest.fixture
def TRANSACTION_PARAM_KEY():
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("airflow.models.Variable.get", lambda key, **kwargs: "transactions")
        yield "transactions"

@pytest.fixture
def mock_postgres_conn_vars(monkeypatch):
    # mock the Postgres connection environment variable
    conn_uri = "postgresql://mock_username:mock_password@mock_host:1234/mock_db_name"
    monkeypatch.setenv("AIRFLOW_CONN_POSTGRES_DB_YT_ELT", conn_uri)
    return Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")

def test_api_param_key(TRANSACTION_PARAM_KEY):
    assert TRANSACTION_PARAM_KEY == "transactions"

def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"