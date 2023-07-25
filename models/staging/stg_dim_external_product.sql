WITH external_stock_item__rename AS (
SELECT  
  stock_item_id AS product_id
  ,category_id AS category_id
FROM `vit-lam-data.wide_world_importers.external__stock_item` 
)

, external_stock_item__cast_data AS (
SELECT
  CAST(product_id AS INTEGER) AS product_id
  , CAST (category_id AS INTEGER) AS category_id
FROM external_stock_item__rename
)

, external_stock_item__add_undefine_record AS (
SELECT
    product_id
    , category_id
FROM external_stock_item__cast_data
UNION ALL
SELECT 
    0 AS product_id
    , 0 AS category_id
, UNION ALL
SELECT
    -1 AS product_id
    , -1 AS category_id
)

SELECT
    product_id
    , category_id
FROM external_stock_item__add_undefine_record