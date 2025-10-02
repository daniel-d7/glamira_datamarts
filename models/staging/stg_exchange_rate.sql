with 

source as (
    select
        *
    from {{ source('raw', 'exchange_rate') }}
),

raw_exchange_rate as (
    select
        farm_fingerprint(symbol) as currency_key,
        symbol as currency_symbol,
        code as currency_code,
        currency_name,
        exchange_rate
    from source
)

select * from raw_exchange_rate