with delivery_method__source as (
SELECT 
*
FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
)

, delivery_method__cast_type as (
select
  cast(delivery_method_id as integer) as delivery_method_id
  , cast(delivery_method_name as string ) as delivery_method_name
from delivery_method__source
)

select
  COALESCE (delivery_method_id, 0) AS delivery_method_id
  ,COALESCE(delivery_method_name , "Undefine") AS delivery_method_name
from delivery_method__cast_type
