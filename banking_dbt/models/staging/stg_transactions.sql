{{ config(
    materialized = 'view'
) }}

with raw_data as (
    select
        v:id::string as transaction_id,
        v:account_id::string as account_id,
        case 
            when v:merchant_id is not null then v:merchant_id::int
            when v:transaction_type::string = 'DEPOSIT' then -1
            when v:transaction_type::string = 'TRANSFER' then -2
            else 0
        end as merchant_id,
        v:amount::float as amount,
        v:transaction_type::string as transaction_type,
        v:related_account_id::string as related_account_id,
        v:status::string as status,
        v:is_high_value::boolean as is_high_value,
        v:cdc_operation::string as cdc_op,
        v:stream_timestamp::timestamp_ntz as stream_ts,
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
    merchant_id,
    amount,
    transaction_type,
    related_account_id,
    status,
    is_high_value,
    transaction_time,
    stream_ts as load_timestamp
from deduplicated
where rn = 1
  and (cdc_op = 'c' or cdc_op = 'r')