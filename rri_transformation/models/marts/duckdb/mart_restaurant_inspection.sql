{{ config(
    materialized='table',
    database='MY_WH',
    schema='mart',
    alias='mart_restaurant_inspection'
) }}

SELECT 
* from {{ref('stg_restaurant_inspection')}}
WHERE inspection_date IS NOT NULL