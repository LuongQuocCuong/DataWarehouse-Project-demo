with stg_location__source as (
SELECT * 
FROM `vit-lam-data.wide_world_importers.application__countries`
)

, stg_location__cast_data as (
SELECT
  cast (country_id as integer) as country_id
  , cast (country_name as string) as country_name
FROM stg_location__source
)

SELECT
  COALESCE(country_id , 0) AS country_id
  ,COALESCE (country_name , "Undefine") AS country_name
FROM stg_location__cast_data
