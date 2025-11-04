-- models/marts/dim_calendar.sql
{{ config(
    materialized='table',
    unique_key='device_key'
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_dim_device') }}
)

SELECT DISTINCT
    s.device_pk
    ,s.resolution
    ,s.operating_system
    ,s.device_name
FROM source s