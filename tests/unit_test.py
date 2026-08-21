from dags.datawarehouse.data_transformation import clean_amount, determine_age_group
from decimal import Decimal

def test_clean_amount_normal():
    assert clean_amount('$-77.00') == Decimal('-77.00')

def test_clean_amount_invalid():
    assert clean_amount(None) is None

def test_age_group():
    assert determine_age_group(30) == '25-34'
    assert determine_age_group(None) == 'Unknown'