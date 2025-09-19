{{ config(
    materialized='table',
    schema='mart',
    alias='mart_rat_risk_model'
) }}

SELECT 
    a.camis,
    a.dba,
    a.boro,
    a.building,
    a.bin,
    a.cuisine_description,
    a.latitude,
    a.longitude,
    a.zipcode,
    a.most_recent_inspection_date,
    a.restaurant_risk_score,
    a.street_risk_score,
    a.zip_risk_score,
    b.bin_rr_score    AS bin_rat_score,
    b.street_rr_score AS street_rat_score,
    b.zip_rr_score    AS zip_rat_score,

    -- Combined weighted rat risk score
    LEAST(100,
        (COALESCE(a.restaurant_risk_score,0) * 0.3) +
        (COALESCE(a.street_risk_score,0) * 0.15) +
        (COALESCE(a.zip_risk_score,0) * 0.05) +
        (COALESCE(b.bin_rr_score,0) * 0.3) +
        (COALESCE(b.street_rr_score,0) * 0.15) +
        (COALESCE(b.zip_rr_score,0) * 0.05)
    ) AS rat_risk_score,

    -- Risk category thresholds
    CASE 
        WHEN LEAST(100,
            (COALESCE(a.restaurant_risk_score,0) * 0.3) +
            (COALESCE(a.street_risk_score,0) * 0.15) +
            (COALESCE(a.zip_risk_score,0) * 0.05) +
            (COALESCE(b.bin_rr_score,0) * 0.3) +
            (COALESCE(b.street_rr_score,0) * 0.15) +
            (COALESCE(b.zip_rr_score,0) * 0.05)
        ) >= 75 THEN 'VERY HIGH RISK'
        
        WHEN LEAST(100,
            (COALESCE(a.restaurant_risk_score,0) * 0.3) +
            (COALESCE(a.street_risk_score,0) * 0.15) +
            (COALESCE(a.zip_risk_score,0) * 0.05) +
            (COALESCE(b.bin_rr_score,0) * 0.3) +
            (COALESCE(b.street_rr_score,0) * 0.15) +
            (COALESCE(b.zip_rr_score,0) * 0.05)
        ) >= 55 THEN 'HIGH RISK'
        
        WHEN LEAST(100,
            (COALESCE(a.restaurant_risk_score,0) * 0.3) +
            (COALESCE(a.street_risk_score,0) * 0.15) +
            (COALESCE(a.zip_risk_score,0) * 0.05) +
            (COALESCE(b.bin_rr_score,0) * 0.3) +
            (COALESCE(b.street_rr_score,0) * 0.15) +
            (COALESCE(b.zip_rr_score,0) * 0.05)
        ) >= 20 THEN 'MODERATE RISK'
        
        ELSE 'LOW RISK'
    END AS risk_category,

    a.extract_date

FROM {{ ref('restaurant_risk_analysis') }} a
LEFT JOIN {{ ref('rat_inspection_analysis') }} b
  ON a.bin = b.bin
