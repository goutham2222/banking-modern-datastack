{{ config(materialized = 'table') }}

WITH latest AS (
    SELECT
        account_id,
        customer_id, 
        account_type,
        account_status,
        balance,
        created_at,
        dbt_valid_from AS effective_from,
        COALESCE(dbt_valid_to, '9999-12-31'::timestamp_ntz) AS effective_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM {{ ref('accounts_snapshot') }}
)

SELECT * FROM latest