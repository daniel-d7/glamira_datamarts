-- models/marts/dim_calendar.sql
{{ config(
    materialized='table',
    unique_key='date_key'
) }}

WITH date_range AS (
    SELECT 
        date_add('2015-01-01', interval day_offset day) AS date_day
    FROM unnest(generate_array(0, date_diff('2030-12-31', '2015-01-01', day))) AS day_offset
),

calendar_base AS (
    SELECT
        format_date('%Y%m%d', date_day) AS date_key
        ,date_day
        ,extract(YEAR FROM date_day) AS year
        ,extract(MONTH FROM date_day) AS month
        ,extract(DAY FROM date_day) AS day
        ,extract(DAYOFWEEK FROM date_day) AS day_of_week
        ,extract(DAYOFYEAR FROM date_day) AS day_of_year
        ,extract(WEEK FROM date_day) AS week_of_year
        ,extract(QUARTER FROM date_day) AS quarter
        ,format_date('%B', date_day) AS month_name
        ,format_date('%b', date_day) AS month_name_short
        ,format_date('%A', date_day) AS day_name
        ,format_date('%a', date_day) AS day_name_short
        ,format_date('%Y-%m', date_day) AS year_month
        ,format_date('%Y-Q%q', date_day) AS year_quarter
        ,concat(cast(extract(YEAR FROM date_day) AS string), '-', lpad(cast(extract(WEEK FROM date_day) AS string), 2, '0')) AS year_week
        ,extract(DAYOFWEEK FROM date_day) in (1, 7) AS is_weekend
        ,extract(DAYOFWEEK FROM date_day) between 2 and 6 AS is_weekday
        ,CASE 
            WHEN extract(DAYOFWEEK FROM date_day) = 2 THEN date_day
            ELSE date_sub(date_day, INTERVAL EXTRACT(DAYOFWEEK FROM date_day) - 2 DAY)
        END AS week_start_date
        ,date_trunc(date_day, MONTH) AS month_start_date
        ,last_day(date_day) AS month_end_date
        ,date_trunc(date_day, QUARTER) AS quarter_start_date
        ,date_trunc(date_day, YEAR) AS year_start_date
        ,extract(day FROM date_day) AS day_of_month
        ,CASE 
            WHEN date_day = last_day(date_day) THEN true 
            ELSE false 
        END AS is_month_end
        ,CASE 
            WHEN date_day = date_trunc(date_day, MONTH) THEN true 
            ELSE false 
        END AS is_month_start
    FROM date_range
),

final AS (
    SELECT
        date_key
        ,date_day
        ,year
        ,month
        ,day
        ,day_of_week
        ,day_of_year
        ,week_of_year
        ,quarter
        ,month_name
        ,month_name_short
        ,day_name
        ,day_name_short
        ,year_month
        ,year_quarter
        ,year_week
        ,is_weekend
        ,is_weekday
        ,week_start_date
        ,month_start_date
        ,month_end_date
        ,quarter_start_date
        ,year_start_date
        ,day_of_month
        ,is_month_end
        ,is_month_start
    FROM calendar_base
)

SELECT * FROM final
ORDER BY date_day
