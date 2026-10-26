select
    transaction_date,
    us_state,
    count() as transaction_count,
    sum(amount) as total_amount,
    avg(amount) as average_amount,
    arrayElement(arraySort(groupArray(amount)), toUInt64(ceil(0.95 * count()))) as p95_amount,
    100.0 * countIf(amount_segment = 'LARGE') / count() as large_transaction_rate
from {{ ref('stg_transactions') }}
group by transaction_date, us_state
