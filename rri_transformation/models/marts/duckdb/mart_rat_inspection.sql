{{ config(materialized='table', database='MY_WH', schema='mart', alias='mart_rat_inspection') }}

SELECT * 
from {{ref('stg_rat_inspection')}}
WHERE inspection_date IS NOT NULL and bin is not null