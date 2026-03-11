{{ config(materialized= 'view') }}

with raw_data as (
    select 
        v:zip_code::string as zip_code,
        v:city::string as city,
        v:state_code::string as state_code,
        v:state_name::string as state_name,
        v:country::string as country,
        v:stream_timestamp::timestamp_ntz as stream_ts
    from {{ source('raw', 'locations') }}
),

deduplicated as (
    select 
        *,
        row_number() over (partition by zip_code order by stream_ts desc) as rn
    from raw_data
)

select
    zip_code,
    city,
    state_code,
    state_name,
    country,
    stream_ts as load_timestamp
from deduplicated
where rn = 1