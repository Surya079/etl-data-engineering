import json
from dags.datawarehouse.data_transformation import transform_transactions
from dags.datawarehouse.data_modification import load_rows_to_staging, load_rows_to_core
from dags.datawarehouse.data_utils import get_conn_cursor, close_conn_cur

def test_full_etl_from_sample_data():
    raw_data = [
        {
            "id": "7475327",
            "date": "2010-01-01 00:01:00",
            "client_id": "1556",
            "card_id": "2972",
            "amount": "$-77.00",
            "use_chip": "Swipe Transaction",
            "merchant_id": "59935",
            "merchant_city": "Beulah",
            "merchant_state": "ND",
            "zip": "58523.0",
            "mcc": "5499",
            "errors": null
        },
        {
            "id": "7475328",
            "date": "2010-01-01 00:02:00",
            "client_id": "742",
            "card_id": "1842",
            "amount": "$125.50",
            "use_chip": "Chip Transaction",
            "merchant_id": "23145",
            "merchant_city": "Bismarck",
            "merchant_state": "ND",
            "zip": "58501.0",
            "mcc": "5812",
            "errors": null
            }
        ]
    transformed = transform_transactions(raw_data)
    stage_rows = [s for s, _ in transformed]
    core_rows = [c for _, c in transformed]

    assert len(core_rows) == 5
    assert core_rows[0]['age_group'] == 'Unknown'
    