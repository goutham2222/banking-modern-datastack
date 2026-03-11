{{ config(materialized= 'view') }}

with raw_data as (
    select 
        v:id::string as account_id,
        v:customer_id::string as customer_id,
        v:account_type::string as account_type,
        v:balance::float as balance,
        v:currency::string as currency,
        v:cdc_operation::string as cdc_op,
        -- Use timestamp_ntz to prevent Snowflake session offsets
        v:stream_timestamp::timestamp_ntz as stream_ts,
        v:created_at::timestamp_ntz as created_at
    from {{ source('raw', 'accounts') }}
),

ranked as (
    select 
        *,
        row_number() over (
            partition by account_id 
            order by stream_ts desc
        ) as rn
    from raw_data
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    stream_ts as load_timestamp
from ranked
where rn = 1
  and cdc_op != 'd' -- Filter out Debezium/Streamer delete flags