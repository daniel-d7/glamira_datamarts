-- models/marts/stg_dim_user.sql
{{ config(
    materialized='table'
) }}

with source as (
    select
        *
    from {{ source('raw', 'glamira_user_event_raw_logs') }}
    where collection = 'checkout_success'
        and user_id_db != ''
        and user_id_db is not null
),

raw_user as (
    select
        user_id_db as user_pk,
        email_address as user_email
    from source
)

select * from raw_user