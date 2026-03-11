{{ config(
    materialized = 'incremental', 
    unique_key = 'transaction_id'
) }}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

accounts AS (
    SELECT * FROM {{ ref('stg_accounts') }}
)

SELECT 
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    SYSDATE()::timestamp_ntz AS load_timestamp
FROM transactions t
LEFT JOIN accounts a
    ON t.account_id = a.account_id

{% if is_incremental() %}
    WHERE t.transaction_time > (SELECT MAX(transaction_time) FROM {{ this }})
{% endif %}