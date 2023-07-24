with color__source as (
SELECT 
*
FROM `vit-lam-data.wide_world_importers.warehouse__colors`
)

, color__source__rename_column as(
select
  color_id as color_id
  ,color_name
from color__source
)

, color__cast_type as (
select
  cast(color_id as integer) as color_id
  , cast(color_name as string ) as color_name
from color__source__rename_column
)


select
  COALESCE(color_id, 0) AS color_id
  ,COALESCE(color_name , "Undefine") AS color_name
from color__cast_type
