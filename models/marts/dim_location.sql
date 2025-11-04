-- models/marts/dim_location.sql
{{ config(
    materialized='table',
    unique_key='location_key'
) }}

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_dim_location') }}
)

SELECT DISTINCT
    location_pk
    ,country_code
    ,region_name
    ,city_name
FROM staging_data