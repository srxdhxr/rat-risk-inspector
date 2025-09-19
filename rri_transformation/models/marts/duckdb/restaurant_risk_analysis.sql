{{ config(
    materialized='table',
    schema='mart',
    alias='mart_restaurant_risk_analysis'
) }}

WITH severe_vio_count AS (
    SELECT 
        camis, dba, bin, inspection_date,
        COUNT(*) FILTER (WHERE violation_code IN (
            '04A','04B','04C','04D','04E','04F','04H','04I','04J','04K','04L','04M','04N','04O','04P',
            '05A','05B','05C','05D','05E'
        )) AS severe_vio_count
    FROM {{ ref('mart_restaurant_inspection') }}
    GROUP BY camis, dba, bin, inspection_date
),
moderate_vio_count AS (
    SELECT 
        camis, dba, bin, inspection_date,
        COUNT(*) FILTER (WHERE violation_code IN (
            '02A','02B','02C','02D','02E','02F','02G','02H','02I','02J',
            '05F','05H','05I',
            '06A','06B','06C','06D','06E','06F','06G','06H','06I'
        )) AS moderate_vio_count
    FROM {{ ref('mart_restaurant_inspection') }}
    GROUP BY camis, dba, bin, inspection_date
),
restaurant_level_stats AS (
    SELECT
        a.camis,
        a.dba,
        a.boro,
        a.building,
        a.street,
        a.zipcode,
        a.inspection_date,
        a.bin,
        a.bbl,
        a.nta,
        a.cuisine_description,
        MAX(v.severe_vio_count)      AS severe_vio_count, 
        MAX(m.moderate_vio_count)    AS moderate_vio_count,
        COUNT(*) FILTER (WHERE critical_flag = 'CRITICAL') AS total_critical_count,
        AVG(computed_score)          AS avg_computed_score,
        MAX(computed_grade)          AS computed_grade,
        MAX(latitude)                AS latitude,
        MAX(longitude)               AS longitude,
        MAX(extract_date)            AS extract_date
    FROM {{ ref('mart_restaurant_inspection') }} a
    JOIN severe_vio_count v 
      ON v.camis = a.camis AND a.inspection_date = v.inspection_date
    JOIN moderate_vio_count m 
      ON m.camis = a.camis AND a.inspection_date = m.inspection_date
    GROUP BY 
        a.camis, a.dba, a.boro, a.building, a.street, a.zipcode, 
        a.inspection_date, a.bin, a.bbl, a.nta, a.cuisine_description
),
restaurant_stats_6m AS (
    SELECT 
        camis, 
        dba,
        boro,
        building,
        street,
        zipcode,
        bin,
        bbl,
        nta,
        cuisine_description,
        MAX(inspection_date) AS most_recent_inspection_date,
        SUM(severe_vio_count) AS total_severe_vio_count_6m,
        SUM(severe_vio_count)::float / COUNT(*) AS avg_severe_vio_count_6m,
        SUM(moderate_vio_count) AS total_moderate_vio_count_6m,
        SUM(moderate_vio_count)::float / COUNT(*) AS avg_moderate_vio_count_6m,
        SUM(avg_computed_score)::float / COUNT(*) AS avg_score_orig_6m,
        COUNT(*) FILTER(WHERE computed_grade = 'A') AS grade_A_count_6m,
        COUNT(*) FILTER(WHERE computed_grade = 'B') AS grade_B_count_6m,
        COUNT(*) FILTER(WHERE computed_grade = 'C') AS grade_C_count_6m,
        MAX(longitude) AS longitude,
        MAX(latitude) AS latitude,
        MAX(extract_date) AS extract_date
    FROM restaurant_level_stats
    WHERE inspection_date > CURRENT_DATE - INTERVAL '6 months'
    GROUP BY camis, dba, boro, building, street, zipcode, bin, bbl, nta, cuisine_description
),
street_stats AS (
    SELECT
        street, 
        SUM(avg_severe_vio_count_6m)/COUNT(camis) AS street_avg_severe_vio_count_6m,
        SUM(avg_moderate_vio_count_6m)/COUNT(camis) AS street_avg_moderate_vio_count_6m,
        SUM(avg_score_orig_6m)/COUNT(*) AS street_avg_score,
        SUM(grade_A_count_6m) AS street_grade_A_count,
        SUM(grade_B_count_6m) AS street_grade_B_count,
        SUM(grade_C_count_6m) AS street_grade_C_count,
        AVG(latitude) AS street_latitude,
        AVG(longitude) AS street_longitude
    FROM restaurant_stats_6m 
    GROUP BY street
),
zip_stats AS (
    SELECT
        zipcode, 
        SUM(avg_severe_vio_count_6m)/COUNT(camis) AS zip_avg_severe_vio_count_6m,
        SUM(avg_moderate_vio_count_6m)/COUNT(camis) AS zip_avg_moderate_vio_count_6m,
        SUM(avg_score_orig_6m)/COUNT(*) AS zip_avg_score,
        SUM(grade_A_count_6m) AS zip_grade_A_count,
        SUM(grade_B_count_6m) AS zip_grade_B_count,
        SUM(grade_C_count_6m) AS zip_grade_C_count,
        AVG(latitude) AS zip_latitude,
        AVG(longitude) AS zip_longitude
    FROM restaurant_stats_6m 
    GROUP BY zipcode
)
SELECT
    r.camis, 
    r.dba,
    r.boro,
    r.building,
    r.street,
    r.zipcode,
    r.bin,
    r.bbl,
    r.nta,
    r.cuisine_description,
    r.most_recent_inspection_date,
    
    -- Restaurant Risk Score Components
    r.avg_severe_vio_count_6m,
    r.avg_moderate_vio_count_6m,
    r.avg_score_orig_6m,
    r.grade_A_count_6m,
    r.grade_B_count_6m,
    r.grade_C_count_6m,
    
    -- Days since last inspection
    DATE_DIFF('day', r.most_recent_inspection_date, CURRENT_DATE) AS days_since_inspection,
    
    -- Restaurant Risk Score
    LEAST(100, 
        COALESCE(r.avg_severe_vio_count_6m * 15, 0) +
        COALESCE(r.avg_moderate_vio_count_6m * 6, 0) +
        COALESCE((r.avg_score_orig_6m - 10) * 0.5, 0) +
        COALESCE(r.grade_C_count_6m * 8, 0) +
        COALESCE(r.grade_B_count_6m * 3, 0) +
        COALESCE(-r.grade_A_count_6m * 2, 0) +
        CASE 
            WHEN DATE_DIFF('day', r.most_recent_inspection_date, CURRENT_DATE) <= 90 THEN 0
            WHEN DATE_DIFF('day', r.most_recent_inspection_date, CURRENT_DATE) <= 180 THEN 3
            WHEN DATE_DIFF('day', r.most_recent_inspection_date, CURRENT_DATE) <= 365 THEN 10
            ELSE 18 + LEAST(15, (DATE_DIFF('day', r.most_recent_inspection_date, CURRENT_DATE) - 365) * 0.05)
        END
    ) AS restaurant_risk_score,
    
    -- Street Risk Score
    LEAST(100,
        COALESCE(s.street_avg_severe_vio_count_6m * 20, 0) +
        COALESCE(s.street_avg_moderate_vio_count_6m * 8, 0) +
        COALESCE((s.street_avg_score - 10) * 1.5, 0) +
        COALESCE((s.street_grade_C_count / NULLIF(s.street_grade_A_count + s.street_grade_B_count + s.street_grade_C_count, 0)) * 30, 0) +
        COALESCE((s.street_grade_B_count / NULLIF(s.street_grade_A_count + s.street_grade_B_count + s.street_grade_C_count, 0)) * 15, 0) -
        COALESCE((s.street_grade_A_count / NULLIF(s.street_grade_A_count + s.street_grade_B_count + s.street_grade_C_count, 0)) * 5, 0)
    ) AS street_risk_score,
    
    -- ZIP Risk Score  
    LEAST(100,
        COALESCE(z.zip_avg_severe_vio_count_6m * 20, 0) +
        COALESCE(z.zip_avg_moderate_vio_count_6m * 8, 0) +
        COALESCE((z.zip_avg_score - 10) * 1.5, 0) +
        COALESCE((z.zip_grade_C_count / NULLIF(z.zip_grade_A_count + z.zip_grade_B_count + z.zip_grade_C_count, 0)) * 30, 0) +
        COALESCE((z.zip_grade_B_count / NULLIF(z.zip_grade_A_count + z.zip_grade_B_count + z.zip_grade_C_count, 0)) * 15, 0) -
        COALESCE((z.zip_grade_A_count / NULLIF(z.zip_grade_A_count + z.zip_grade_B_count + z.zip_grade_C_count, 0)) * 5, 0)
    ) AS zip_risk_score,
    
    r.longitude,
    r.latitude,
    r.extract_date

FROM restaurant_stats_6m r
LEFT JOIN street_stats s ON r.street = s.street  
LEFT JOIN zip_stats z ON r.zipcode = z.zipcode
ORDER BY restaurant_risk_score DESC
