{{config(materialized='table',database='MY_WH', schema='stage')}}

SELECT * FROM {{ref('clean_restaurant_inspection')}} where inspection_date = '1900-01-01 00:00:00'

