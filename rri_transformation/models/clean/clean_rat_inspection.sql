-- /Users/sridhar/Dev/rat-risk-inspector/rri_transformation/models/clean/clean_rat_inspection.sql
{{ config(
    materialized='table',
    schema='clean'
) }}

-- Creates a unique inspection_order_id by combining camis, bin, violation_code, and inspection_date
-- This helps track unique inspection events and their violations

WITH deduplicated AS (
    SELECT 
        inspection_type,
        job_ticket_or_work_order_id,
        job_id,
        job_progress,
        bbl,
        boro_code,
        block,
        lot,
        house_number,
        street_name,
        zip_code,
        latitude,
        longitude,
        borough,
        CAST(inspection_date AS TIMESTAMP) as inspection_date,
        result,
        CAST(approved_date AS TIMESTAMP) as approved_date, 
        bin,
        nta,
        ROW_NUMBER() OVER (
            PARTITION BY job_ticket_or_work_order_id 
            ORDER BY 
                CAST(inspection_date as datetime) DESC,
                CAST(approved_date as datetime) DESC
        ) as row_num
    FROM {{ source('raw', 'rat_inspection') }}
)

SELECT 
    upper(inspection_type) as inspection_type,
    cast(job_ticket_or_work_order_id as integer) as work_order_id,
    job_id,
    cast(job_progress as integer) as job_progress,
    cast(bbl as bigint) as bbl,
    cast(boro_code as integer) as boro_code,
    block,
    lot,
    house_number,
    street_name as street,
    zip_code as zipcode,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    upper(borough) as borough,
    inspection_date,
    UPPER(result) as result,
    approved_date,
    cast(bin as integer) as bin,
    nta,
    current_date as extract_date
FROM deduplicated
WHERE row_num = 1
  AND inspection_date BETWEEN TIMESTAMP '2000-01-01' AND current_date