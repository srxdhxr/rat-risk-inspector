{{config(materialized='table',database='MY_WH', schema='stage')}}

with lat_lon_from_rest_inspection as (
    select distinct bin, latitude, longitude from {{ref('clean_restaurant_inspection')}} where latitude is not null and longitude is not null
)
select distinct * from (
    SELECT 
    a.inspection_type,
    a.work_order_id,
    a.job_id,
    a.job_progress,
    a.bbl,
    a.boro_code,
    a.block,
    a.lot,
    a.house_number,
    a.street,
    a.zipcode,
    coalesce(a.latitude, b.latitude) as latitude,
    coalesce(a.longitude, b.longitude) as longitude,
    a.borough,
    a.inspection_date,
    a.result,
    a.approved_date,
    a.bin,
    a.nta,
    CURRENT_DATE as extract_date
     FROM {{ref('clean_rat_inspection')}} a left join lat_lon_from_rest_inspection b
     on a.bin = b.bin and a.latitude = b.latitude and a.longitude = b.longitude
      where a.inspection_date BETWEEN DATE '2000-01-01' AND current_date and a.bin is not null
)
