with 

parsed_user_agents as (
  select
    device_id,

    resolution,

    case 
      when regexp_contains(user_agent, r'iPhone OS (\d+_\d+)') then 'iOS'
      when regexp_contains(user_agent, r'Mac OS X (\d+[_\d]*)') then 'macOS'
      when regexp_contains(user_agent, r'Android (\d+(?:\.\d+)*)') then 'Android'
      when regexp_contains(user_agent, r'Windows NT (\d+\.\d+)') then 'Windows'
      when regexp_contains(user_agent, r'Linux') then 'Linux'
      else 'Unknown OS'
    end as operating_system,

    case 
      when regexp_contains(user_agent, r'SAMSUNG ([^)]+)') then 
        regexp_extract(user_agent, r'SAMSUNG ([^)]+)')
      when regexp_contains(user_agent, r'iPhone') then 'iPhone'
      when regexp_contains(user_agent, r'Redmi Note 8 Pro') then 'Redmi Note 8 Pro'
      when regexp_contains(user_agent, r'Macintosh') then 'Mac'
      when regexp_contains(user_agent, r'Linux; Android.*; ([^)]+)') then
        regexp_extract(user_agent, r'Linux; Android.*; ([^)]+)')
      else 'Unknown Device'
    end as device_name

  from {{ source('raw', 'glamira_user_event_raw_logs') }}
    where device_id != ''
        and device_id is not null
)

select * from parsed_user_agents