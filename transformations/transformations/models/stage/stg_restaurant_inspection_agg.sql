{{config(materialized='table',database='MY_WH', schema='stage')}}


SELECT 
    camis, 
    dba, 
    dba_lower, 
    boro,
    building, 
    street, 
    zipcode, 
    phone, 
    inspection_date,
    count(*) FILTER (WHERE critical_flag = 'CRITICAL')      AS critical_violations,
    count(*) FILTER (WHERE critical_flag = 'NOT CRITICAL')  AS not_critical_violations,

    max(record_date)           AS record_date,
    max(bin)                   AS bin,
    max(bbl)                   AS bbl,
    max(nta)                   AS nta,
    max(cuisine_description)   AS cuisine_description,
    list(violation_code)       AS violation_codes,
    list(violation_description) AS violation_descriptions,
    sum(score)                 AS total_score,
    max(grade)                 AS grade,
    max(grade_date)            AS grade_date,
    max(latitude)              AS latitude,
    max(longitude)             AS longitude,
    max(extract_date)          AS extract_date

FROM {{ref('stg_restaurant_inspection')}}
GROUP BY 
    camis, dba, dba_lower, boro, building, street, zipcode, phone, inspection_date
