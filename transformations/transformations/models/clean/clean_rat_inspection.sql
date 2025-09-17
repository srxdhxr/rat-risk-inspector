{{config(materialized='table', database = 'MY_WH',schema='clean')}}


select 
  upper(inspection_type) as inspection_type,
  cast(job_ticket_or_work_order_id as integer) as work_order_id,
  job_id,
  cast(job_progress as integer) as job_progress,
  cast(bbl as bigint) as bbl,
  cast(boro_code as integer) as boro_code,
  block,
  lot,
  house_number,
  street_name,
  zip_code,
  cast(latitude as double) as latitude,
  cast(longitude as double) as longitude,
  upper(borough) as borough,
  cast(inspection_date as datetime) as inspection_date,
  UPPER(result) as result,
  cast(approved_date as datetime) as approved_date,
  cast(bin as integer) as bin,
  nta
from {{ source('raw', 'rat_inspection') }}

