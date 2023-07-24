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

, delivery_method__add_undefined_record AS(
select
  delivery_method_id
  , delivery_method_name
from delivery_method__cast_type
UNION ALL
SELECT
    0 AS delivery_method_id
    , 'Undefined' AS delivery_method_name
, UNION ALL
SELECT
    -1 AS delivery_method_id
    , 'Invalid' AS delivery_method_name
)

SELECT
    delivery_method_id
    , delivery_method_name
FROM delivery_method__add_undefined_record