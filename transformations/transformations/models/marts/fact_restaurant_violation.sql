{{ config(materialized='table', database='MY_WH', schema='mart') }}

SELECT
    camis,
    inspection_date,
    violation_code,
    violation_description
FROM {{ ref('stg_restaurant_inspection') }}
WHERE violation_code IS NOT NULL
