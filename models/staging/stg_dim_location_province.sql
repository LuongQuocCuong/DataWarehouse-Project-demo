with stg_location__source as (
SELECT * 
FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, stg_location__rename_column as(
SELECT
  state_province_id as province_id
  , state_province_name as province_name
  , country_id as country_id
FROM stg_location__source
)

, stg_location__cast_data as (
SELECT
  cast (province_id as integer) as province_id
  , cast (province_name as string) as province_name
  , cast (country_id as integer) as country_id
FROM stg_location__rename_column
)

SELECT 
  COALESCE(province_id , 0) AS province_id
  , COALESCE(province_name , "Undefine") AS province_name
  , COALESCE(country_id , 0) AS country_id
FROM stg_location__cast_data