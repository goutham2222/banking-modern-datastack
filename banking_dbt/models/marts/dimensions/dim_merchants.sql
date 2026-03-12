{{ config(materialized = 'table') }}

SELECT 
    merchant_id,
    merchant_name,
    merchant_category,
    load_timestamp
FROM {{ ref('stg_merchants') }}