{{ config(materialized = 'view') }}

with raw_data as (
    select
        v:id::string as transaction_id,
        v:account_id::string as account_id,
        case 
            when v:merchant_id is not null then v:merchant_id::int
            when v:transaction_type::string = 'SALARY' or v:transaction_type::string = 'DEPOSIT' then -1
            when v:transaction_type::string = 'ZELLE' then -3
            when v:transaction_type::string = 'ACH' then -4
            when v:transaction_type::string = 'WIRE' then -5
            else 0
        end as merchant_id,
        v:amount::float as amount,
        v:transaction_type::string as transaction_type,
        v:related_account_id::string as related_account_id,
        COALESCE(v:status::string, 'COMPLETED') as status, -- Handle legacy records
        v:is_high_value::boolean as is_high_value,
        v:cdc_operation::string as cdc_op,
        v:stream_timestamp::timestamp_ntz as stream_ts,
        to_timestamp_ntz(v:created_at::int, 6) as transaction_time
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
    cdc_op,
    stream_ts
from deduplicated
where rn = 1
  and (cdc_op = 'c' or cdc_op = 'r')