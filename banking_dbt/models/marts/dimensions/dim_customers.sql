{{ config(materialized = 'table') }}

SELECT 
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
    dbt_valid_from AS effective_from,
    COALESCE(dbt_valid_to, '9999-12-31'::timestamp_ntz) AS effective_to,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM {{ ref('customers_snapshot') }}