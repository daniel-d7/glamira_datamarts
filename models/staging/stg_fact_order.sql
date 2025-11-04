-- models/marts/stg_fact_order.sql
{{ config(
    materialized='table'
) }}

WITH raw_sales AS (
    SELECT
        *
    FROM {{ source('raw', 'glamira_user_event_raw_logs') }}
    WHERE collection = 'checkout_success'
        AND cart_products IS NOT NULL
)

,location_joint AS (
    SELECT
        location_pk
        ,ip_address
    FROM {{ ref('stg_dim_location') }}
)

,raw_sales AS (
    SELECT
        CAST(FORMAT_DATE('%Y%m%d', CAST(time_stamp AS DATE)) AS INT64) AS date_key
        ,CAST(local_time AS STRING) AS local_time
        ,s.order_id as order_pk
        ,DISTINCT FARM_FINGERPRINT(CONCAT(COALESCE(TRIM(SAFE_CAST(ip AS STRING)), ''), COALESCE(TRIM(SAFE_CAST(order_id AS STRING)), ''), COALESCE(TRIM(SAFE_CAST(product_id AS STRING)), ''))) AS sales_hash_key
        ,s.user_id_db AS user_pk
        ,s.device_id AS device_pk
        ,lj.location_pk
        ,s.store_code AS store_pk
        ,COALESCE(cp.product_id, -1) AS product_pk
        ,SUM(SAFE_CAST(cp.amount AS INT64)) AS product_quantity
        ,AVG(COALESCE(SAFE_CAST(TRIM(cp.price) AS FLOAT64), 0.0)) AS product_price
        ,COALESCE(NULLIF(TRIM(cp.currency), ''), 'Unknown') AS product_currency
        ,SUM(SAFE_CAST(cp.amount AS INT64)) * AVG(COALESCE(SAFE_CAST(TRIM(cp.price) AS FLOAT64), 0.0)) AS line_total
    FROM source s
    CROSS JOIN UNNEST(cart_products) AS cp
    INNER JOIN location_joint lj ON s.ip = lj.ip_address
    WHERE cp.product_id IS NOT NULL
)