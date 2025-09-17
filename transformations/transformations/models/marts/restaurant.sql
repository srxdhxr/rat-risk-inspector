{{config(database = 'MY_WH', schema = 'mart', materialized = 'table')}}

SELECT
  camis,
  max(dba) as dba,
  max(dba_lower) as dba_lower,
  max(boro) as boro,
  max(building) as building,
  max(street) as street,
  max(zipcode) as zipcode,
  max(phone) as phone,

  count(distinct inspection_date) as total_inspections,
  max(inspection_date) as recent_inspection_date,
  min(inspection_date) as oldest_inspection_date,

  max(record_date) as record_date,
  max(bin) as bin,
  max(bbl) as bbl,
  max(nta) as nta,
  max(cuisine_description) as cuisine_description,
  max(latitude) as latitude,
  max(longitude) as longitude,
  max(extract_date) as extract_date

FROM {{ref('stg_restaurant_inspection')}}
GROUP BY camis