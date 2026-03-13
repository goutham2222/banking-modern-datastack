{{ config(materialized= 'view') }}

with raw_data as (
    select 
        v:id::string as account_id,
        v:customer_id::string as customer_id,
        v:account_type::string as account_type,
        v:account_status::string as account_status,
        v:balance::float as balance,
        v:currency::string as currency,
        v:cdc_operation::string as cdc_op,
        v:stream_timestamp::timestamp_ntz as stream_ts,
        to_timestamp_ntz(v:created_at::int, 6) as created_at
    from {{ source('raw', 'accounts') }}
),

ranked as (
    select 
        *,
        row_number() over (partition by account_id order by stream_ts desc) as rn
    from raw_data
)

select
    account_id,
    customer_id,
    account_type,
    account_status,
    balance,
    currency,
    created_at,
    stream_ts as load_timestamp
from ranked
where rn = 1
  and cdc_op != 'd'