select
    customer_id,
    count() as transaction_count,
    sum(fraud_flag) as fraud_count,
    100.0 * sum(fraud_flag) / count() as fraud_rate,
    avg(amount) as average_amount,
    sum(amount) as total_amount,
    min(transaction_time) as first_transaction,
    max(transaction_time) as last_transaction,
    case
        when 100.0 * sum(fraud_flag) / count() >= 5 then 'HIGH'
        when sum(fraud_flag) > 0 then 'MEDIUM'
        else 'LOW'
    end as risk_level
from {{ ref('stg_transactions') }}
group by customer_id
