-- models/marts/dim_calendar.sql
{{ config(
    materialized='table',
    labels={'domain': 'calendar', 'layer': 'marts'}
) }}

with date_range as (
    select 
        date_add('2015-01-01', interval day_offset day) as date_day
    from unnest(generate_array(0, date_diff('2030-12-31', '2015-01-01', day))) as day_offset
),

calendar_base as (
    select
        -- Primary key
        format_date('%Y%m%d', date_day) as date_key,
        date_day,
        
        -- Date parts
        extract(year from date_day) as year,
        extract(month from date_day) as month,  
        extract(day from date_day) as day,
        extract(dayofweek from date_day) as day_of_week,
        extract(dayofyear from date_day) as day_of_year,
        extract(week from date_day) as week_of_year,
        extract(quarter from date_day) as quarter,
        
        -- Formatted names
        format_date('%B', date_day) as month_name,
        format_date('%b', date_day) as month_name_short,
        format_date('%A', date_day) as day_name,
        format_date('%a', date_day) as day_name_short,
        
        -- Composite identifiers
        format_date('%Y-%m', date_day) as year_month,
        format_date('%Y-Q%q', date_day) as year_quarter,
        concat(
            cast(extract(year from date_day) as string), 
            '-', 
            lpad(cast(extract(week from date_day) as string), 2, '0')
        ) as year_week,
        
        -- Boolean flags
        extract(dayofweek from date_day) in (1, 7) as is_weekend,
        extract(dayofweek from date_day) between 2 and 6 as is_weekday,
        
        -- Additional useful fields
        case 
            when extract(dayofweek from date_day) = 2 then date_day  -- Monday
            else date_sub(date_day, interval extract(dayofweek from date_day) - 2 day)
        end as week_start_date,
        
        date_trunc(date_day, month) as month_start_date,
        last_day(date_day) as month_end_date,
        date_trunc(date_day, quarter) as quarter_start_date,
        date_trunc(date_day, year) as year_start_date,
        
        -- Day position in period
        extract(day from date_day) as day_of_month,
        case 
            when date_day = last_day(date_day) then true 
            else false 
        end as is_month_end,
        
        case 
            when date_day = date_trunc(date_day, month) then true 
            else false 
        end as is_month_start
        
    from date_range
),

final as (
    select
        date_key,
        date_day,
        year,
        month,
        day,
        day_of_week,
        day_of_year,
        week_of_year,
        quarter,
        month_name,
        month_name_short,
        day_name,
        day_name_short,
        year_month,
        year_quarter,
        year_week,
        is_weekend,
        is_weekday,
        week_start_date,
        month_start_date,
        month_end_date,
        quarter_start_date,
        year_start_date,
        day_of_month,
        is_month_end,
        is_month_start
    from calendar_base
)

select * from final
order by date_day
