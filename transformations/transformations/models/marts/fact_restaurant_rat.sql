{{ config(
    materialized='table',
    database='MY_WH',
    schema='mart'
) }}

WITH rest AS (
    SELECT 
        camis, dba, boro, building, street, zipcode, bin, latitude, longitude 
    FROM {{ ref('dim_restaurant') }}
),

-- Restaurant inspection stats (6 months)
rest_insp_avg AS (
    SELECT 
        r.camis, r.dba, r.boro, r.street, r.zipcode, r.bin, r.latitude, r.longitude,
        AVG(fi.high_severity_violation_count) AS avg_high_severity_6m,
        AVG(fi.moderate_high_severity_violation_count) AS avg_mod_high_severity_6m,
        AVG(fi.moderate_severity_violation_count) AS avg_mod_severity_6m,
        AVG(fi.average_score) AS avg_score_6m,
        COUNT(DISTINCT fi.inspection_date) AS inspection_count_6m
    FROM {{ ref('fact_restaurant_inspection') }} fi
    JOIN rest r ON r.camis = fi.camis
    WHERE fi.inspection_date >= current_date - interval '6 months'
    GROUP BY r.camis, r.dba, r.boro, r.street, r.zipcode, r.bin, r.latitude, r.longitude
),

-- Consistent rat data using dim_rat_inspection for all levels
-- BIN level rat stats
rat_bin AS (
    SELECT 
        bin,
        -- Current period (6 months)
        AVG(CASE WHEN result = 'RAT ACTIVITY' THEN 1 ELSE 0 END) AS rat_activity_rate_6m,
        AVG(CASE WHEN result = 'PASSED' THEN 1 ELSE 0 END) AS rat_pass_rate_6m,
        COUNT(*) AS total_rat_inspections_6m,
        MAX(inspection_date) AS recent_rat_insp_date,
        
        -- Trend comparison (6m ago to 12m ago)
        AVG(CASE 
            WHEN result = 'RAT ACTIVITY' 
            AND inspection_date >= current_date - interval '12 months'
            AND inspection_date < current_date - interval '6 months'
            THEN 1 ELSE 0 
        END) AS rat_activity_rate_6m_prior,
        
        -- Recency weight (more recent = higher weight)
        AVG(CASE 
            WHEN result = 'RAT ACTIVITY' 
            THEN (1.0 - DATEDIFF('day', inspection_date, current_date) / 180.0) -- Weight decreases over 6 months
            ELSE 0 
        END) AS weighted_rat_activity_6m
        
    FROM {{ ref('dim_rat_inspection') }}
    WHERE inspection_date >= current_date - interval '12 months' -- Include both periods
    GROUP BY bin
),

-- Street level rat stats  
rat_street AS (
    SELECT 
        street_name,
        zip_code,
        AVG(CASE WHEN result = 'RAT ACTIVITY' THEN 1 ELSE 0 END) AS street_rat_activity_rate_6m,
        AVG(CASE WHEN result = 'PASSED' THEN 1 ELSE 0 END) AS street_rat_pass_rate_6m,
        COUNT(*) AS street_total_rat_inspections_6m,
        COUNT(DISTINCT bin) AS street_unique_bins_inspected_6m,
        MAX(inspection_date) AS street_recent_rat_insp_date
    FROM {{ ref('dim_rat_inspection') }}
    WHERE inspection_date >= current_date - interval '6 months'
    GROUP BY street_name, zip_code
),

-- ZIP level rat stats
rat_zip AS (
    SELECT 
        zip_code,
        AVG(CASE WHEN result = 'RAT ACTIVITY' THEN 1 ELSE 0 END) AS zip_rat_activity_rate_6m,
        AVG(CASE WHEN result = 'PASSED' THEN 1 ELSE 0 END) AS zip_rat_pass_rate_6m,
        COUNT(*) AS zip_total_rat_inspections_6m,
        COUNT(DISTINCT bin) AS zip_unique_bins_inspected_6m,
        COUNT(DISTINCT street_name) AS zip_unique_streets_inspected_6m,
        MAX(inspection_date) AS zip_recent_rat_insp_date
    FROM {{ ref('dim_rat_inspection') }}
    WHERE inspection_date >= current_date - interval '6 months'
    GROUP BY zip_code
),

-- Street level restaurant averages
rest_insp_street AS (
    SELECT 
        r.street, r.zipcode,
        AVG(fi.high_severity_violation_count) AS street_avg_high_severity_6m,
        AVG(fi.moderate_high_severity_violation_count) AS street_avg_mod_high_severity_6m,
        AVG(fi.moderate_severity_violation_count) AS street_avg_mod_severity_6m,
        AVG(fi.average_score) AS street_avg_score_6m
    FROM {{ ref('fact_restaurant_inspection') }} fi
    JOIN rest r ON r.camis = fi.camis
    WHERE fi.inspection_date >= current_date - interval '6 months'
    GROUP BY r.street, r.zipcode
),

-- ZIP level restaurant averages  
rest_insp_zip AS (
    SELECT 
        r.zipcode,
        AVG(fi.high_severity_violation_count) AS zip_avg_high_severity_6m,
        AVG(fi.moderate_high_severity_violation_count) AS zip_avg_mod_high_severity_6m,
        AVG(fi.moderate_severity_violation_count) AS zip_avg_mod_severity_6m,
        AVG(fi.average_score) AS zip_avg_score_6m
    FROM {{ ref('fact_restaurant_inspection') }} fi
    JOIN rest r ON r.camis = fi.camis
    WHERE fi.inspection_date >= current_date - interval '6 months'
    GROUP BY r.zipcode
)

SELECT 
    ria.camis,
    ria.dba,
    ria.boro,
    ria.street,
    ria.zipcode,
    ria.bin,
    ria.latitude,
    ria.longitude,

    -- Restaurant inspection metrics
    ria.avg_high_severity_6m,
    ria.avg_mod_high_severity_6m,
    ria.avg_mod_severity_6m,
    ria.avg_score_6m,
    ria.inspection_count_6m,

    -- Street level restaurant metrics
    ris.street_avg_high_severity_6m,
    ris.street_avg_mod_high_severity_6m,
    ris.street_avg_mod_severity_6m,
    ris.street_avg_score_6m,

    -- ZIP level restaurant metrics
    riz.zip_avg_high_severity_6m,
    riz.zip_avg_mod_high_severity_6m,
    riz.zip_avg_mod_severity_6m,
    riz.zip_avg_score_6m,

    -- BIN level rat metrics (consistent calculation)
    rb.rat_activity_rate_6m,
    rb.rat_pass_rate_6m,
    rb.total_rat_inspections_6m,
    rb.recent_rat_insp_date,
    rb.rat_activity_rate_6m_prior,
    rb.weighted_rat_activity_6m,
    
    -- Calculated trend indicator
    CASE 
        WHEN rb.rat_activity_rate_6m_prior > 0 
        THEN (rb.rat_activity_rate_6m - rb.rat_activity_rate_6m_prior) / rb.rat_activity_rate_6m_prior
        ELSE NULL 
    END AS rat_activity_trend_6m,

    -- Street level rat metrics
    rs.street_rat_activity_rate_6m,
    rs.street_rat_pass_rate_6m,
    rs.street_total_rat_inspections_6m,
    rs.street_unique_bins_inspected_6m,
    rs.street_recent_rat_insp_date,

    -- ZIP level rat metrics
    rz.zip_rat_activity_rate_6m,
    rz.zip_rat_pass_rate_6m,
    rz.zip_total_rat_inspections_6m,
    rz.zip_unique_bins_inspected_6m,
    rz.zip_unique_streets_inspected_6m,
    rz.zip_recent_rat_insp_date,

    -- Composite rat risk score (example calculation)
    COALESCE(
        (rb.weighted_rat_activity_6m * 0.6) +           -- Recent activity (60% weight)
        (rs.street_rat_activity_rate_6m * 0.3) +        -- Street context (30% weight) 
        (rz.zip_rat_activity_rate_6m * 0.1),            -- ZIP context (10% weight)
        0
    ) AS composite_rat_risk_score

FROM rest_insp_avg ria
LEFT JOIN rest_insp_street ris ON ria.street = ris.street AND ria.zipcode = ris.zipcode
LEFT JOIN rest_insp_zip riz ON ria.zipcode = riz.zipcode
LEFT JOIN rat_bin rb ON ria.bin = rb.bin
LEFT JOIN rat_street rs ON ria.street = rs.street_name AND ria.zipcode = rs.zip_code
LEFT JOIN rat_zip rz ON ria.zipcode = rz.zip_code