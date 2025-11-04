-- models/marts/stg_dim_user.sql
{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        *
    FROM {{ source('raw', 'glamira_user_event_raw_logs') }}
    WHERE user_id_db != ''
        AND user_id_db IS NOT NULL
),

raw_user AS (
    SELECT
        user_id_db AS user_pk,
        email_address AS user_email
    FROM source
)

SELECT * FROM raw_user