select
    us_state,
    count() as transaction_count,
    sum(fraud_flag) as fraud_count,
    100.0 * sum(fraud_flag) / count() as fraud_rate,
    uniqExact(customer_id) as customer_count,
    uniqExact(merchant) as merchant_count,
    sum(amount) as total_amount,
    sumIf(amount, fraud_flag = 1) as fraud_amount
from {{ ref('stg_transactions') }}
group by us_state
