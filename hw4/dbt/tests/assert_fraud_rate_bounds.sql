select category
from {{ ref('mart_fraud_by_category') }}
where fraud_rate < 0 or fraud_rate > 100 or fraud_count > transaction_count
