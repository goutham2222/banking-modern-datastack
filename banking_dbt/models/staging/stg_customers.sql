{{ config(materialized= 'view') }}

with raw_data as (
    select 
        v:id::string as customer_id,
        v:first_name::string as first_name,
        v:last_name::string as last_name,
        v:email::string as email,
        v:cdc_operation::string as cdc_op, -- Use the new metadata
        v:stream_timestamp::timestamp as stream_ts, -- Use the new metadata
        v:created_at::timestamp as created_at
    from {{ source('raw', 'customers') }}
),

ranked as (
    select 
        *,
        row_number() over (
            partition by customer_id 
            order by stream_ts desc -- Order by the most recent stream event
        ) as rn
    from raw_data
)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    stream_ts as load_timestamp
from ranked
where rn = 1 
  and cdc_op != 'd'