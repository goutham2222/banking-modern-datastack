{{ config(materialized = 'view') }}

with raw_data as (
    select
        v:id::string as transaction_id,
        v:account_id::string as account_id,
        v:amount::float as amount,
        v:transaction_type::string as transaction_type,
        v:related_account_id::string as related_account_id,
        v:status::string as status,
        v:cdc_operation::string as cdc_op, -- Metadata from streamer
        v:stream_timestamp::timestamp as stream_ts, -- Metadata from streamer
        v:created_at::timestamp as transaction_time
    from {{ source('raw', 'transactions') }}
),

deduplicated as (
    select
        *,
        -- In case of duplicate ingestion, pick the record that was streamed last
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
  and cdc_op = 'c' -- Usually, we only want 'Create' events for transactions