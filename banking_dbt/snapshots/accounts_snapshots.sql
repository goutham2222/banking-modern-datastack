{{
    config(
        materialized='snapshot',
        target_schema='ANALYTICS',
        unique_key='account_id',
        strategy='check',
        check_cols=['customer_id', 'account_type', 'balance']
    )
}}

select *
from {{ ref('stg_accounts') }}