{{ config(materialized = 'table') }}

-- 1. Pull real merchants from your Silver layer (Staging)
WITH staging_merchants AS (
    SELECT 
        merchant_id, 
        merchant_name, 
        merchant_category 
    FROM {{ ref('stg_merchants') }}
),

-- 2. Pull the "Dummy" records from the CSV file you just created
manual_merchants AS (
    SELECT 
        merchant_id, 
        merchant_name, 
        merchant_category 
    FROM {{ ref('manual_merchants') }}
)

-- 3. Combine them into one final Dimension table
SELECT * FROM staging_merchants
UNION ALL
SELECT * FROM manual_merchants