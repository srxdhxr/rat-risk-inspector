{{config(materialized='table', database = 'MY_WH',schema='clean')}}


with source_data as (
select
    -- Create a unique inspection order ID
    MD5(CONCAT(
        COALESCE(camis, ''),
        COALESCE(bin, ''),
        COALESCE(violation_code, ''),
        COALESCE(inspection_date, '')
    )) as inspection_order_id,
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
    UPPER(violation_code) as violation_code,
    violation_description,
    cast(score as integer) as score,
    grade,
    cast(grade_date as datetime) as grade_date,
    cast(latitude as decimal(18,12)) as latitude,
    cast(longitude as decimal(18,12)) as longitude,
    current_date as extract_date
from {{ source('raw', 'restaurant_inspection') }}
),

deduplicated as (
    select 
        *,
        row_number() over (
            partition by inspection_order_id 
            order by 
                inspection_date desc, 
                record_date desc
        ) as row_num
    from source_data
    where inspection_date is not null
)

select * from deduplicated
where row_num = 1