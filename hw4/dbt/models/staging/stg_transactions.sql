with source_data as (
    select * from {{ source('raw', 'transactions') }}
)

select
    toString(transaction_id) as transaction_id,
    toDateTime(transaction_time, 'UTC') as transaction_time,
    toDate(transaction_time) as transaction_date,
    toUInt8({{ dbt_date.day_of_week('transaction_time', isoweek=true) }}) as day_of_week,
    toUInt8(toHour(transaction_time)) as transaction_hour,
    UPPER(trimBoth(us_state)) as us_state,
    trimBoth(cat_id) as category,
    trimBoth(merch) as merchant,
    UPPER(trimBoth(gender)) as gender,
    toFloat64(amount) as amount,
    toUInt8(target) as fraud_flag,
    {{ amount_bucket('amount') }} as amount_segment,
    {{ dbt_utils.generate_surrogate_key(['name_1', 'name_2', 'lat', 'lon']) }} as customer_id
from source_data
