{{ config(materialized = 'view') }}

with raw_data as (
    select
        -- Remove payload:after: because the data is already flat
        v:id::string as transaction_id,
        v:account_id::string as account_id,
        v:amount::float as amount,
        v:transaction_type::string as transaction_type,
        v:related_account_id::string as related_account_id,
        v:status::string as status,
        v:cdc_operation::string as cdc_op,
        v:stream_timestamp::timestamp_ntz as stream_ts,
        -- created_at is at the root level now
        v:created_at::timestamp_ntz as transaction_time
    from {{ source('raw', 'transactions') }}
),

deduplicated as (
    select
        *,
        row_number() over(
            partition by transaction_id 
            order by stream_ts desc
        ) as rn
    from raw_data
)

select 
    transaction_id,
    account_id,
    amount,
    transaction_type,
    related_account_id,
    status,
    transaction_time,
    stream_ts as load_timestamp
from deduplicated
where rn = 1
  -- Use 'r' for Snapshot or 'c' for Create
  and (cdc_op = 'c' or cdc_op = 'r')