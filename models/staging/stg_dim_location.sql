with 

source as (

    select * from {{ source('raw', 'ip_locations') }}

),

raw_location as (

    select
        farm_fingerprint(concat(country, region, city)) as location_key,
        country as country_code,
        region as region_name,
        city as city_name,
        ip as ip_address

    from source

)

select * from raw_location
