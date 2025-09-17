    {{ config(database = 'MY_WH', schema = 'mart', materialized = 'table') }}

    SELECT 
    bin,
    inspection_date,

    count(*) FILTER (WHERE inspection_type = 'INITIAL')    AS initial_inspection_count,
    count(*) FILTER (WHERE inspection_type = 'COMPLIANCE') AS compliance_inspection_count,
    count(*) FILTER (WHERE inspection_type = 'BAIT')       AS bait_inspection_count,

    count(*)                          AS total_inspections_that_day,

    count(*) FILTER (WHERE result = 'PASSED')       AS inspection_pass_count,
    count(*) FILTER (WHERE result = 'RAT ACTIVITY') AS rat_activity_report_count,
    count(*) FILTER (WHERE result = 'BAIT')         AS bait_application_count,

    max(approved_date) AS most_recent_approved_date
    FROM {{ ref('stg_rat_inspection') }}
    GROUP BY bin, inspection_date
