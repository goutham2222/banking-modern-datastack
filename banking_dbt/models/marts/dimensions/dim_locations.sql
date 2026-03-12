{{ config(materialized = 'table') }}

SELECT 
    zip_code,
    city,
    state_code,
    state_name,
    country,
    load_timestamp
FROM {{ ref('stg_locations') }}