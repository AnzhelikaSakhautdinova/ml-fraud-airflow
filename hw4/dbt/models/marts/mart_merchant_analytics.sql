select
    merchant,
    count() as transaction_count,
    sum(fraud_flag) as fraud_count,
    100.0 * sum(fraud_flag) / count() as fraud_rate,
    sum(amount) as total_amount,
    avg(amount) as average_amount,
    toUInt8(count() >= 10 and 100.0 * sum(fraud_flag) / count() >= 1) as suspicious_flag
from {{ ref('stg_transactions') }}
group by merchant
