{{ config(materialized= 'view') }}

with raw_data as (
    select 
        v:id::string as customer_id,
        v:first_name::string as first_name,
        v:last_name::string as last_name,
        v:email::string as email,
        v:birth_date::date as birth_date,
        v:address::string as address,
        v:zip_code::string as zip_code,
        v:marital_status::string as marital_status,
        v:education_level::string as education_level,
        v:income_category::string as income_category,
        v:estimated_net_worth::float as estimated_net_worth,
        v:employment_status::string as employment_status,
        v:cdc_operation::string as cdc_op,
        v:stream_timestamp::timestamp_ntz as stream_ts,
        v:created_at::timestamp_ntz as created_at
    from {{ source('raw', 'customers') }}
),

ranked as (
    select 
        *,
        row_number() over (partition by customer_id order by stream_ts desc) as rn
    from raw_data
)

select
    customer_id,
    first_name,
    last_name,
    email,
    birth_date,
    address,
    zip_code,
    marital_status,
    education_level,
    income_category,
    estimated_net_worth,
    employment_status,
    created_at,
    stream_ts as load_timestamp
from ranked
where rn = 1 
  and cdc_op != 'd'