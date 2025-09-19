{{config(materialized='table',database='MY_WH', schema='stage')}}


with avg_scores as (
    select 
        camis,
        inspection_date,
        AVG(SCORE) as avg_score
    from {{ref('clean_restaurant_inspection')}}
    group by camis, inspection_date
)

select distinct * from (

select 
  a.inspection_order_id,
  a.camis,
  a.dba,
  a.dba_lower,
  a.boro,
  a.building,
  a.street,
  a.zipcode,
  a.inspection_date,
  a.critical_flag,
  a.record_date,
  a.bin,
  a.bbl,
  a.nta,
  a.cuisine_description,
  a."action",
  a.violation_code,
  a.violation_description,
  a.score,
  b.avg_score as computed_score,
  CASE WHEN a.inspection_date >= CURRENT_DATE - INTERVAL '1 months' THEN 'Z' ELSE 'N' END as grade,
  CASE WHEN b.avg_score  >=0 AND b.avg_score  <=13 THEN 'A'
       WHEN b.avg_score  >13 AND b.avg_score  <=27 THEN 'B'
       WHEN b.avg_score  >27 THEN 'C'
       ELSE 'N' END as computed_grade,
  coalesce(a.grade_date,a.inspection_date) as grade_date, 
  a.latitude,
  a.longitude,
  a.extract_date
from {{ref('clean_restaurant_inspection')}} A
left join avg_scores b
  on a.camis = b.camis and a.inspection_date = b.inspection_date

where a.inspection_date != '1900-01-01 00:00:00' and a.bin is not null and a.zipcode is not null and a.violation_code is not null
)

