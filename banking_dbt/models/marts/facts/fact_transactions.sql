{{ config(
    materialized = 'incremental', 
    unique_key = 'transaction_id'
) }}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

accounts AS (
    SELECT account_id, customer_id FROM {{ ref('stg_accounts') }}
)

SELECT 
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.merchant_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.is_high_value,
    t.transaction_time,
    SYSDATE()::timestamp_ntz AS load_timestamp 
FROM transactions t
LEFT JOIN accounts a
    ON t.account_id = a.account_id

{% if is_incremental() %}
    WHERE t.transaction_time > (
        SELECT COALESCE(MAX(transaction_time), '1900-01-01'::timestamp_ntz) 
        FROM {{ this }}
    )
{% endif %}