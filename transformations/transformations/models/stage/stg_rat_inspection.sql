{{config(materialized='table',database='MY_WH', schema='stage')}}

select distinct * from (
    SELECT * FROM {{ref('clean_rat_inspection')}} where inspection_date BETWEEN DATE '2000-01-01' AND current_date
)

