with supplier_category__source as (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.purchasing__supplier_categories`
)

, supplier_category__cast_data as (
SELECT
  cast(supplier_category_id as integer) as supplier_category_id
  ,cast (supplier_category_name as string) as supplier_category_name
FROM supplier_category__source
)

, supplier_category__add_undifined_record as (
SELECT 
  supplier_category_id
  , supplier_category_name
FROM supplier_category__cast_data
UNION ALL 
  SELECT
    0 AS supplier_category_id
    , 'Undefined' AS supplier_category_name
, UNION ALL
  SELECT
    -1 AS supplier_category_id
    , 'Invalid' AS supplier_category_name
)

select 
  supplier_category_id
  , supplier_category_name
FROM supplier_category__add_undifined_record