select 'daily totals differ from staging' as problem
where (
    select sum(transaction_count) from {{ ref('mart_daily_state_metrics') }}
) != (
    select count() from {{ ref('stg_transactions') }}
)
