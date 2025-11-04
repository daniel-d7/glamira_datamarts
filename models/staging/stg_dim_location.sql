-- models/marts/stg_dim_location.sql
{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'ip_locations') }}
),

raw_location AS (
    SELECT
        farm_fingerprint(concat(country, region, city)) AS location_pk
        ,country AS country_code
        ,region AS region_name
        ,city AS city_name
        ,ip AS ip_address
    FROM source
)

SELECT * FROM raw_location
