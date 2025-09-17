{{config(materialized='table', database='MY_WH', schema='mart')}}

SELECT DISTINCT
  camis,
  dba,
  dba_lower,
  boro,
  building,
  street,
  zipcode,
  phone,
  bin,
  bbl,
  nta,
  cuisine_description,
  latitude,
  longitude,
  extract_date
FROM {{ ref('stg_restaurant_inspection')}}
