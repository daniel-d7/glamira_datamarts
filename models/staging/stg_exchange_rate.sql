-- models/marts/dim_exchange_rate.sql
{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        *
    FROM {{ source('raw', 'exchange_rate') }}
),

raw_exchange_rate AS (
    SELECT
        farm_fingerprint(symbol) AS currency_pk
        ,symbol AS currency_symbol
        ,code AS currency_code
        ,currency_name
        ,exchange_rate
    FROM source
)

SELECT
    currency_pk
    ,currency_symbol
    ,currency_code
    ,currency_name
    ,exchange_rate
FROM raw_exchange_rate