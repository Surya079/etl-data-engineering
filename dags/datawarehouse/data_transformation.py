import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime

logger = logging.getLogger(__name__)

def clean_amount(amount):
    """
    Convert a raw amount value (str, float, Decimal, int) to Decimal.
    Returns None if the value cannot be converted.
    """
    if amount is None:
        return None

    # Already a Decimal – return as is
    if isinstance(amount, Decimal):
        return amount

    # Numeric types (float, int) – convert via string to avoid float precision issues
    if isinstance(amount, (int, float)):
        try:
            return Decimal(str(amount))
        except InvalidOperation:
            return None

    # String – strip currency symbols and commas, then parse
    if isinstance(amount, str):
        try:
            return Decimal(amount.replace('$', '').replace(',', '').strip())
        except InvalidOperation:
            return None

    # Unsupported type
    return None
def determine_age_group(age):
    if age is None:
        return 'Unknown'
    if age < 25:
        return '18-24'
    elif age < 35:
        return '25-34'
    elif age < 45:
        return '35-44'
    elif age < 55:
        return '45-54'
    elif age < 65:
        return '55-64'
    else:
        return '65+'

def build_core_row(transaction, user_info, mcc_mapping, fraud_label):
    """
    Build a core row dict from transaction data, user info, mcc mapping, and fraud label.
    Works for both fresh API data and staging rows.
    """
    # Transaction ID
    txn_id = int(transaction.get('id') if 'id' in transaction else transaction.get('transaction_id'))
    # Date handling
    txn_date = transaction.get('date')
    if isinstance(txn_date, str):
        txn_date = datetime.strptime(txn_date, '%Y-%m-%d %H:%M:%S')
    elif hasattr(txn_date, 'date'):
        txn_date = txn_date
    else:
        txn_date = None

    client_id = int(transaction.get('client_id', 0))

    # User fields (support both dict and flat row)
    if isinstance(user_info, dict):
        current_age = int(user_info.get('current_age', 0)) if user_info.get('current_age') else None
        gender = user_info.get('gender', 'Unknown')
        per_capita_income = clean_amount(user_info.get('per_capita_income', '$0'))
        yearly_income = clean_amount(user_info.get('yearly_income', '$0'))
        total_debt = clean_amount(user_info.get('total_debt', '$0'))
        credit_score = int(user_info.get('credit_score', 0))
        num_credit_cards = int(user_info.get('num_credit_cards', 0))
    else:
        current_age = user_info.get('current_age')
        gender = user_info.get('gender', 'Unknown')
         # ---- Income / Debt ----
        per_capita_income = clean_amount(user_info.get('per_capita_income', '$0')) or Decimal('0.0')
        yearly_income = clean_amount(user_info.get('yearly_income', '$0')) or Decimal('0.0')
        total_debt = clean_amount(user_info.get('total_debt', '$0')) or Decimal('0.0')
        credit_score = user_info.get('credit_score')
        # ---- Other numeric fields ----
        num_credit_cards = int(user_info.get('num_credit_cards', 0)) if user_info.get('num_credit_cards') else 0

    
    if current_age is not None:
        age_group = determine_age_group(current_age)
    else:
        age_group = 'Unknown'


    # Amount
    amount = clean_amount(transaction.get('amount', '0'))
    if amount is None:
        amount = Decimal('0.0')

    # ---- Transaction type ----
    txn_type = transaction.get('use_chip', 'Other')
    if txn_type not in ('Swipe Transaction', 'Chip Transaction', 'Online Transaction'):
        txn_type = 'Other'

    # ---- Credit score ----
    
    if credit_score is None:
        credit_score = 0
    else:
        credit_score = int(credit_score)

    # Merchant
    merchant_id = int(transaction.get('merchant_id', 0))
    merchant_city = transaction.get('merchant_city', '')
    merchant_state = transaction.get('merchant_state', '')
    merchant_zip = str(transaction.get('zip', '')).replace('.0', '')

    mcc = int(transaction.get('mcc', 0))
    if mcc_mapping:
        category = mcc_mapping.get(str(mcc), 'Unknown')
    else:
        category = transaction.get('merchant_category', 'Unknown')

    is_fraud_str = fraud_label if fraud_label else 'No'
    is_fraud_bool = (is_fraud_str.lower() == 'yes')

    core_row = {
         'transaction_id': int(transaction.get('id', transaction.get('transaction_id'))),
        'transaction_date': txn_date.date() if txn_date else None,
        'client_id': client_id,
        'age_group': age_group,
        'gender': user_info.get('gender', 'Unknown'),
        'city': merchant_city or 'Unknown',
        'state': merchant_state or 'Unknown',
        'per_capita_income': per_capita_income,
        'yearly_income': yearly_income,
        'total_debt': total_debt,
        'credit_score': credit_score,
        'credit_cards_count': num_credit_cards,
        'card_id': int(transaction.get('card_id', 0)),
        'amount': amount,
        'transaction_type': txn_type,
        'merchant_id': merchant_id,
        'merchant_city': merchant_city,
        'merchant_state': merchant_state,
        'merchant_zip': merchant_zip,
        'mcc': mcc,
        'category': category,
        'is_fraud': is_fraud_bool,
        'error_flag': bool(transaction.get('errors')),
    }
    return core_row

def transform_transactions(raw_data):
    """
    Input: dict with keys 'transactions', 'users', 'merchant_categories', 'fraud_labels', 'metadata'
    Output: list of tuples (staging_row, core_row)
    """
    # --- EXTRACT LISTS FROM PAGINATED WRAPPERS ---
    transactions_raw = raw_data.get('transactions', [])
    if isinstance(transactions_raw, dict) and 'data' in transactions_raw:
        transactions = transactions_raw['data']          # list of txn dicts
    else:
        transactions = transactions_raw if isinstance(transactions_raw, list) else []

    users_raw = raw_data.get('users', [])
    if isinstance(users_raw, dict) and 'data' in users_raw:
        users_list = users_raw['data']
    else:
        users_list = users_raw if isinstance(users_raw, list) else []

    fraud_raw = raw_data.get('fraud_labels', [])
    if isinstance(fraud_raw, dict) and 'data' in fraud_raw:
        fraud_labels = fraud_raw['data']
    else:
        fraud_labels = fraud_raw if isinstance(fraud_raw, list) else []

    mcc_mapping = raw_data.get('merchant_categories', {})
    # -------------------------------------------------

    # Index users by client_id
    users_by_id = {}
    for u in users_list:
        cid = str(u.get('id'))
        users_by_id[cid] = u

    # Index fraud by transaction_id
    fraud_by_id = {}
    for f in fraud_labels:
        tid = str(f.get('transaction_id'))
        fraud_by_id[tid] = f.get('is_fraud', 'No')

    transformed_rows = []

    for txn in transactions:
        txn_id = int(txn['id'])
        client_id = int(txn['client_id'])
        user_info = users_by_id.get(str(client_id), {})
        fraud_label = fraud_by_id.get(str(txn_id), 'No')

        # Staging row – keep all raw fields joined
        staging_row = {
            'transaction_id': txn_id,
            'transaction_date': datetime.strptime(txn['date'], '%Y-%m-%d %H:%M:%S'),
            'client_id': client_id,
            'current_age': int(user_info.get('current_age', 0)) if user_info.get('current_age') else None,
            'retirement_age': int(user_info.get('retirement_age', 0)) if user_info.get('retirement_age') else None,
            'birth_year': int(user_info.get('birth_year', 0)) if user_info.get('birth_year') else None,
            'birth_month': int(user_info.get('birth_month', 0)) if user_info.get('birth_month') else None,
            'gender': user_info.get('gender', 'Unknown'),
            'address': user_info.get('address', ''),
            'latitude': float(user_info['latitude']) if user_info.get('latitude') else None,
            'longitude': float(user_info['longitude']) if user_info.get('longitude') else None,
            'per_capita_income': clean_amount(user_info.get('per_capita_income', '$0')),
            'yearly_income': clean_amount(user_info.get('yearly_income', '$0')),
            'total_debt': clean_amount(user_info.get('total_debt', '$0')),
            'credit_score': int(user_info.get('credit_score', 0)),
            'num_credit_cards': int(user_info.get('num_credit_cards', 0)),
            'card_id': int(txn.get('card_id', 0)),
            'amount': clean_amount(txn.get('amount', '0')),
            'use_chip': txn.get('use_chip', 'Swipe Transaction'),
            'merchant_id': int(txn.get('merchant_id', 0)),
            'merchant_city': txn.get('merchant_city', ''),
            'merchant_state': txn.get('merchant_state', ''),
            'merchant_zip': str(txn.get('zip', '')).replace('.0', ''),
            'mcc': int(txn.get('mcc', 0)),
            'merchant_category': mcc_mapping.get(str(txn.get('mcc', 0)), 'Unknown'),
            'errors': txn.get('errors'),
            'is_fraud': fraud_label
        }

        # Core row – use the shared builder
        core_row = build_core_row(txn, user_info, mcc_mapping, fraud_label)

        transformed_rows.append((staging_row, core_row))

    return transformed_rows

def transform_staging_to_core(staging_row):
    """
    Transform a staging row (already joined) into a core row.
    """
    # The staging row already contains all fields – we pass it as transaction and user_info
    # and use the existing is_fraud value.
    return build_core_row(
        staging_row,              # as transaction
        staging_row,              # as user_info
        None,                     # mcc_mapping (category already set)
        staging_row.get('is_fraud')
    )