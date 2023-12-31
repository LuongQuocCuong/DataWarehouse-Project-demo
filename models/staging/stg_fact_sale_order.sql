WITH stg_sale_order__source AS (
SELECT *
FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_sale_order__rename AS (
SELECT
  order_id AS order_id
  , customer_id 
  , salesperson_person_id AS sales_person_id
  , picked_by_person_id AS picked_by_person_id
  , contact_person_id AS contact_person_id
  , order_date
  , expected_delivery_date
  , picking_completed_when
  , is_undersupply_backordered
FROM stg_sale_order__source
)

, stg_sale_order__cast_data AS (
SELECT
  CAST (order_id AS integer) AS order_id
  , CAST(customer_id AS integer) AS customer_id
  , CAST (sales_person_id AS integer) AS sales_person_id
  , CAST (picked_by_person_id AS integer) AS picked_by_person_id
  , CAST (contact_person_id AS integer) AS contact_person_id
  , CAST (order_date AS date) AS order_date
  , CAST (expected_delivery_date AS date) AS expected_delivery_date
  , CAST (picking_completed_when AS date) AS picking_completed_when
  , CAST (is_undersupply_backordered AS boolean) AS is_undersupply_backordered_boolean
FROM stg_sale_order__rename
)

, stg_sale_order__convert_boolean AS (
SELECT
  *
  , CASE
      WHEN is_undersupply_backordered_boolean is TRUE then 'Under Supply Back Ordered'
      WHEN is_undersupply_backordered_boolean is FALSE then 'Not Under Supply Back Ordered'
      ELSE 'Invalid'
    END AS is_undersupply_back_ordered
  , CASE
      WHEN is_undersupply_backordered_boolean is TRUE then 'true'
      WHEN is_undersupply_backordered_boolean is FALSE then 'false'
      ELSE 'Invalid'
    END AS is_undersupply_back_ordered_boolean
FROM stg_sale_order__cast_data
)

, stg_sale_order__Unifined_handle AS (
SELECT
  COALESCE (order_id , 0) AS order_id
  , COALESCE (customer_id , 0) AS customer_id
  , COALESCE (sales_person_id , 0) AS sales_person_id
  , COALESCE (picked_by_person_id , 0) AS picked_by_person_id
  , COALESCE (contact_person_id , 0) AS contact_person_id
  , order_date
  , expected_delivery_date
  , picking_completed_when
  , is_undersupply_back_ordered
  , is_undersupply_back_ordered_boolean
FROM stg_sale_order__convert_boolean
) 

, stg_sale_order__add_undifined_record AS (
SELECT
  order_id
  , customer_id
  , sales_person_id
  , picked_by_person_id
  , contact_person_id
  , order_date
  , expected_delivery_date
  , picking_completed_when
  , is_undersupply_back_ordered
  , is_undersupply_back_ordered_boolean
FROM stg_sale_order__Unifined_handle
UNION ALL
  SELECT
    0 AS order_id
    ,0 AS customer_id
    ,0 AS sales_person_id
    ,0 AS picked_by_person_id
    ,0 AS contact_person_id
    , CAST (NULL AS DATE) AS order_date
    , CAST (NULL AS DATE) AS expected_delivery_date
    , CAST (NULL AS DATE) AS picking_completed_when
    , 'Undefined' AS is_undersupply_back_ordered
    , 'undefined' AS is_undersupply_back_ordered_boolean
, UNION ALL
  SELECT
    -1 AS order_id
    ,-1 AS customer_id
    ,-1 AS sales_person_id
    ,-1 AS picked_by_person_id
    ,-1 AS contact_person_id
    , CAST (NULL AS DATE) AS order_date
    , CAST (NULL AS DATE) AS expected_delivery_date
    , CAST (NULL AS DATE) AS picking_completed_when
    , 'Invalid' AS is_undersupply_back_ordered
    , 'Invalid' AS is_undersupply_back_ordered_boolean
)
SELECT 
  order_id
  , customer_id
  , sales_person_id
  , picked_by_person_id
  , contact_person_id
  , order_date
  , expected_delivery_date
  , picking_completed_when
  , is_undersupply_back_ordered
  , is_undersupply_back_ordered_boolean
FROM stg_sale_order__add_undifined_record