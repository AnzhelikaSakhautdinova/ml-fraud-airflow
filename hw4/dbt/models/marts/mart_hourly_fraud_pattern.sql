select
    day_of_week,
    transaction_hour,
    count() as transaction_count,
    sum(fraud_flag) as fraud_count,
    100.0 * sum(fraud_flag) / count() as fraud_rate,
    sum(amount) as total_amount
from {{ ref('stg_transactions') }}
group by day_of_week, transaction_hour
