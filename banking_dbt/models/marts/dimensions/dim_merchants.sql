{{ config(materialized = 'table') }}

WITH staging_merchants AS (
    SELECT 
        merchant_id, 
        merchant_name, 
        merchant_category 
    FROM {{ ref('stg_merchants') }}
),

manual_merchants AS (
    SELECT 
        merchant_id, 
        merchant_name, 
        merchant_category 
    FROM {{ ref('manual_merchants') }}
)

SELECT * FROM staging_merchants
UNION ALL
SELECT * FROM manual_merchants