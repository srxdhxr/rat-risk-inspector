{{ config(
    materialized='table',
    schema='mart',
    alias='mart_rat_inspection_analysis'
) }}

WITH building_stats_6m AS (
    SELECT 
        bin,
        street,
        zipcode,
        bbl,
        boro_code,
        block,
        lot,
        house_number,
        borough,
        nta,
        MAX(inspection_date) AS most_recent_inspection_date,
        MAX(approved_date) AS most_recent_approved_date,
        
        -- Inspection outcome metrics
        COUNT(*) AS total_inspections_6m,
        COUNT(*) FILTER (WHERE result = 'FAILED') AS failed_inspections_6m,
        COUNT(*) FILTER (WHERE result = 'PASSED') AS passed_inspections_6m,
        COUNT(*) FILTER (WHERE result = 'RAT ACTIVITY') AS rat_activities_6m,
        
        -- Inspection type analysis
        COUNT(*) FILTER (WHERE inspection_type ILIKE '%INITIAL%') AS initial_inspections_6m,
        COUNT(*) FILTER (WHERE inspection_type ILIKE '%BAIT%') AS bait_inspections_6m,
        COUNT(*) FILTER (WHERE inspection_type ILIKE '%COMPLIANCE%') AS compliance_inspections_6m,
        
        -- Calculate rates
        COUNT(*) FILTER (WHERE result = 'FAILED FOR OTHER R')::float / NULLIF(COUNT(*), 0) AS failure_rate_6m,
        COUNT(*) FILTER (WHERE result = 'RAT ACTIVITY')::float / NULLIF(COUNT(*), 0) AS rat_activity_rate_6m,
        
        MAX(latitude) AS latitude,
        MAX(longitude) AS longitude,
        MAX(CURRENT_DATE) AS extract_date
    FROM {{ ref('mart_rat_inspection') }}
    WHERE inspection_date > CURRENT_DATE - INTERVAL '6 months'
    GROUP BY bin, street, zipcode, bbl, boro_code, block, lot, house_number, borough, nta
),

street_stats AS (
    SELECT
        street,
        COUNT(DISTINCT bin) AS buildings_on_street,
        AVG(failure_rate_6m) AS street_avg_failure_rate,
        AVG(rat_activity_rate_6m) AS street_avg_rat_activity_rate,
        SUM(failed_inspections_6m)::float / NULLIF(SUM(total_inspections_6m), 0) AS street_failure_rate,
        SUM(rat_activities_6m)::float / NULLIF(SUM(total_inspections_6m), 0) AS street_rat_activity_rate,
        SUM(initial_inspections_6m)::float / NULLIF(SUM(total_inspections_6m), 0) AS street_initial_rate,
        SUM(bait_inspections_6m)::float / NULLIF(SUM(total_inspections_6m), 0) AS street_bait_rate,
        SUM(compliance_inspections_6m)::float / NULLIF(SUM(total_inspections_6m), 0) AS street_compliance_rate,
        AVG(total_inspections_6m) AS street_avg_inspections,
        AVG(latitude) AS street_latitude,
        AVG(longitude) AS street_longitude
    FROM building_stats_6m
    GROUP BY street
),

zip_stats AS (
    SELECT
        zipcode,
        COUNT(DISTINCT bin) AS buildings_in_zip,
        AVG(failure_rate_6m) AS zip_avg_failure_rate,
        AVG(rat_activity_rate_6m) AS zip_avg_rat_activity_rate,
        SUM(failed_inspections_6m)::float / NULLIF(SUM(total_inspections_6m), 0) AS zip_failure_rate,
        SUM(rat_activities_6m)::float / NULLIF(SUM(total_inspections_6m), 0) AS zip_rat_activity_rate,
        SUM(initial_inspections_6m)::float / NULLIF(SUM(total_inspections_6m), 0) AS zip_initial_rate,
        SUM(bait_inspections_6m)::float / NULLIF(SUM(total_inspections_6m), 0) AS zip_bait_rate,
        SUM(compliance_inspections_6m)::float / NULLIF(SUM(total_inspections_6m), 0) AS zip_compliance_rate,
        AVG(total_inspections_6m) AS zip_avg_inspections,
        AVG(latitude) AS zip_latitude,
        AVG(longitude) AS zip_longitude
    FROM building_stats_6m
    GROUP BY zipcode
)

SELECT
    b.bin,
    b.street,
    b.zipcode,
    b.latitude,
    b.longitude,
    
    -- BIN Risk Score
    LEAST(100,
        COALESCE(b.rat_activity_rate_6m * 35, 0) +               
        COALESCE(b.failure_rate_6m * 25, 0) +                    
        COALESCE((b.failed_inspections_6m::float / NULLIF(b.total_inspections_6m, 0)) * 20, 0) + 
        COALESCE((b.initial_inspections_6m::float / NULLIF(b.total_inspections_6m, 0)) * 15, 0) + 
        COALESCE((b.bait_inspections_6m::float / NULLIF(b.total_inspections_6m, 0)) * 10, 0) +    
        COALESCE((b.compliance_inspections_6m::float / NULLIF(b.total_inspections_6m, 0)) * 12, 0) + 
        CASE 
            WHEN b.most_recent_inspection_date IS NULL THEN 20
            WHEN DATE_DIFF('day', b.most_recent_inspection_date, CURRENT_DATE) <= 30 THEN 0
            WHEN DATE_DIFF('day', b.most_recent_inspection_date, CURRENT_DATE) <= 90 THEN 2
            WHEN DATE_DIFF('day', b.most_recent_inspection_date, CURRENT_DATE) <= 180 THEN 5
            WHEN DATE_DIFF('day', b.most_recent_inspection_date, CURRENT_DATE) <= 365 THEN 12
            ELSE 20 + LEAST(15, (DATE_DIFF('day', b.most_recent_inspection_date, CURRENT_DATE) - 365) * 0.05)
        END +
        CASE 
            WHEN b.total_inspections_6m = 0 THEN 15
            WHEN b.total_inspections_6m <= 1 AND b.rat_activities_6m > 0 THEN 8
            WHEN b.total_inspections_6m <= 2 AND (b.failed_inspections_6m > 0 OR b.rat_activities_6m > 0) THEN 5
            ELSE 0
        END
    ) AS bin_rr_score,
    
    -- Street Risk Score
    LEAST(100,
        COALESCE(s.street_rat_activity_rate * 40, 0) +           
        COALESCE(s.street_failure_rate * 20, 0) +                
        COALESCE(s.street_initial_rate * 15, 0) +                
        COALESCE(s.street_bait_rate * 12, 0) +                   
        COALESCE(s.street_compliance_rate * 18, 0) +             
        CASE 
            WHEN s.buildings_on_street <= 2 THEN 8
            WHEN s.buildings_on_street <= 1 THEN 15
            ELSE 0
        END
    ) AS street_rr_score,
    
    -- ZIP Risk Score
    LEAST(100,
        COALESCE(z.zip_rat_activity_rate * 40, 0) +              
        COALESCE(z.zip_failure_rate * 20, 0) +                   
        COALESCE(z.zip_initial_rate * 15, 0) +                    
        COALESCE(z.zip_bait_rate * 12, 0) +                      
        COALESCE(z.zip_compliance_rate * 18, 0) +                
        CASE 
            WHEN z.buildings_in_zip <= 5 THEN 5
            WHEN z.buildings_in_zip <= 2 THEN 10
            ELSE 0
        END
    ) AS zip_rr_score,
    
    b.extract_date

FROM building_stats_6m b
LEFT JOIN street_stats s ON b.street = s.street
LEFT JOIN zip_stats z ON b.zipcode = z.zipcode
ORDER BY bin_rr_score DESC
