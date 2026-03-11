{{ config(materialized= 'view') }}

with raw_data as (
    select 
        v:id::int as merchant_id,
        v:name::string as merchant_name,
        v:category::string as merchant_category,
        v:stream_timestamp::timestamp_ntz as stream_ts
    from {{ source('raw', 'merchants') }}
),

deduplicated as (
    select 
        *,
        row_number() over (partition by merchant_id order by stream_ts desc) as rn
    from raw_data
)

select
    merchant_id,
    merchant_name,
    merchant_category,
    stream_ts as load_timestamp
from deduplicated
where rn = 1