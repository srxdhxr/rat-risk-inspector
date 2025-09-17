{{ config(database='MY_WH', schema='mart', materialized='table') }}

SELECT DISTINCT
    bin,
    inspection_date,
    inspection_type,
    result,
    approved_date,
    bbl,
    boro_code,
    block,
    lot,
    house_number,
    street_name,
    zip_code,
    borough,
    nta,
    latitude,
    longitude
FROM {{ ref('stg_rat_inspection') }}
