-- models/marts/dim_location.sql
{{ config(
    materialized='table',
    unique_key='location_key'
) }}

with staging_data as (
    select * from {{ ref('stg_dim_location') }}
)

select distinct
    location_key,
    country_code,
    region_name,
    city_name,
from staging_data