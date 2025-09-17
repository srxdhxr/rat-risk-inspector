{{config(materialized='table', database = 'MY_WH',schema='clean')}}

select
    cast(camis as integer) as camis,
    dba as dba,
    lower(dba) as dba_lower,
    upper(boro) as boro, 
    building as building,
    street as street,
    zipcode as zipcode,
    phone as phone,
    cast(inspection_date as datetime) as inspection_date,
    upper(critical_flag) as critical_flag,
    cast(record_date as datetime) as record_date,
    cast(bin as integer) as bin,
    bbl as bbl,
    nta,
    cuisine_description,
    action, 
    violation_code,
    violation_description,
    cast(score as integer) as score,
    grade,
    cast(grade_date as datetime) as grade_date,
    cast(latitude as decimal(18,12)) as latitude,
    cast(longitude as decimal(18,12)) as longitude,
    current_date as extract_date
from {{ source('raw', 'restaurant_inspection') }}