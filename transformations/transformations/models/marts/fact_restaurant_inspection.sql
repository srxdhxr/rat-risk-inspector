{{ config(database = 'MY_WH', schema = 'mart', materialized = 'table') }}

SELECT
  camis,
  inspection_date,

  -- inspection aggregates
  count(*) FILTER (WHERE critical_flag = 'CRITICAL')      AS critical_violations,
  count(*) FILTER (WHERE critical_flag = 'NOT CRITICAL')  AS not_critical_violations,
  sum(score)                                              AS total_score,
  avg(score)                                              AS average_score,
  COALESCE(max(grade),'N')                                AS grade,
  COALESCE(max(grade_date), inspection_date)              AS recent_grade_date,
  min(grade_date)                                         AS oldest_grade_date,

  -- severity buckets
  count(*) FILTER (WHERE violation_code IN (
      '04A','04B','04C','04D','04E','04F','04H','04I','04J','04K','04L','04M','04N','04O','04P',
      '05A','05B','05C','05D','05E'
  )) AS high_severity_violation_count,

  count(*) FILTER (WHERE violation_code IN (
      '02A','02B','02C','02D','02E','02F','02G','02H','02I','02J',
      '05F','05H','05I',
      '06A','06B','06C','06D','06E','06F','06G','06H','06I'
  )) AS moderate_high_severity_violation_count,

  count(*) FILTER (WHERE violation_code IN (
      '08A','08B','08C','09A','09B','09C','09D','09E',
      '10A','10B','10C','10D','10E','10F','10G','10H','10I','10J'
  )) AS moderate_severity_violation_count,

  count(*) FILTER (WHERE violation_code IN (
      '15A1','15E2','15E3','15F1','15F2','15F6','15F7','15I','15L','15S',
      '15-01','15-17','15-21','15-22','15-27','15-32','15-33','15-36','15-37','15-39','15-42',
      '16A','16B','16C','16D','16E','16F','16G','16H','16I','16J','16K','16L',
      '16-01','16-02','16-03','16-04','16-06','16-08','16-09','16-10','16-11',
      '18-01','18-02','18-08','18-11','18-12','18-13','18-14',
      '19-01','19-03','19-04','19-05','19-06','19-07','19-08','19-10','19-11',
      '20A','20B','20C','20D','20E','20F','20-01','20-02','20-04','20-05','20-06','20-07','20-08'
  )) AS low_severity_violation_count,

  -- keep raw lists for debugging / detailed exploration
  list(violation_code)          AS violation_codes,
  list(violation_description)   AS violation_descriptions

FROM {{ ref('stg_restaurant_inspection') }}
GROUP BY camis, inspection_date
