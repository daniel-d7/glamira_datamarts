-- models/marts/stg_dim_device.sql
{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        *
    FROM {{ source('raw', 'glamira_user_event_raw_logs') }}
    WHERE device_id != ''
        AND device_id IS NOT NULL
),

parsed_user_agents AS (
  SELECT
    device_id AS device_pk
    ,resolution
    ,CASE 
      WHEN regexp_contains(user_agent, r'iPhone OS (\d+_\d+)') THEN 'iOS'
      WHEN regexp_contains(user_agent, r'Mac OS X (\d+[_\d]*)') THEN 'macOS'
      WHEN regexp_contains(user_agent, r'Android (\d+(?:\.\d+)*)') THEN 'Android'
      WHEN regexp_contains(user_agent, r'Windows NT (\d+\.\d+)') THEN 'Windows'
      WHEN regexp_contains(user_agent, r'Linux') THEN 'Linux'
      ELSE 'Unknown OS'
    END AS operating_system
    ,CASE 
      WHEN regexp_contains(user_agent, r'SAMSUNG ([^)]+)') THEN regexp_extract(user_agent, r'SAMSUNG ([^)]+)')
      WHEN regexp_contains(user_agent, r'iPhone') THEN 'iPhone'
      WHEN regexp_contains(user_agent, r'Redmi Note 8 Pro') THEN 'Redmi Note 8 Pro'
      WHEN regexp_contains(user_agent, r'Macintosh') THEN 'Mac'
      WHEN regexp_contains(user_agent, r'Linux; Android.*; ([^)]+)') THEN regexp_extract(user_agent, r'Linux; Android.*; ([^)]+)')
      ELSE 'Unknown Device'
    END AS device_name
  FROM source
)

SELECT * FROM parsed_user_agents