with stg_location__source as (
SELECT * 
FROM `vit-lam-data.wide_world_importers.application__cities`
)

, stg_location__rename_column as(
SELECT
  city_id 
  , city_name
  , state_province_id as province_id
FROM stg_location__source
)

, stg_location__cast_data as (
SELECT
  cast (city_id as integer) as city_id
  , cast (city_name as string) as city_name
  , cast (province_id as integer) as province_id
FROM stg_location__rename_column
)

SELECT
  city_id
  , city_name
  , province_id 
FROM stg_location__cast_data
