def test_api_param_key(TRANSACTION_PARAM_KEY):
    assert TRANSACTION_PARAM_KEY == "transactions"

def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars

    
    assert conn.login=="mock_username"
    assert conn.password=="mock_password"
    assert conn.host=="mock_host"
    assert conn.port==1234
    assert conn.schema=="mock_db_name"