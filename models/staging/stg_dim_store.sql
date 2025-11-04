-- models/marts/stg_dim_store.sql
{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        store_id
    FROM {{ source('raw', 'glamira_user_event_raw_logs') }}
    WHERE store_id IS NOT NULL
)

,raw_store AS (
    SELECT
        store_id AS store_pk
        ,concat('store_', store_id) AS store_name
    FROM source
)

SELECT * FROM raw_store