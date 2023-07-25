WITH fact_purchase_order__source AS (
SELECT * 
FROM `vit-lam-data.wide_world_importers.purchasing__purchase_orders`
)

, fact_purchase_order__rename_column AS (
SELECT
  purchase_order_id as purchase_order_id
  , supplier_id as supplier_id
  , order_date
  , delivery_method_id as delivery_method_id
  , contact_person_id as contact_person_id 
  , expected_delivery_date as expected_delivery_date
  , is_order_finalized
FROM fact_purchase_order__source
)

, fact_purchase_order__cast_data AS(
SELECT
  CAST(purchase_order_id AS INTEGER) AS purchase_order_id
  , CAST(supplier_id AS INTEGER) AS supplier_id
  , CAST(order_date AS DATE) AS order_date
  , CAST(delivery_method_id AS INTEGER) AS delivery_method_id
  , CAST(contact_person_id AS INTEGER) AS contact_person_id
  , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
  , CAST(is_order_finalized AS BOOLEAN) AS is_order_finalized_boolean
FROM fact_purchase_order__rename_column
)

, fact_purchase_order__handle_null AS (
SELECT 
  purchase_order_id
  , COALESCE(supplier_id , 0) AS supplier_id
  , COALESCE(delivery_method_id , 0) AS delivery_method_id
  , COALESCE(contact_person_id , 0) AS contact_person_id
  , order_date
  , expected_delivery_date
  , is_order_finalized_boolean
FROM fact_purchase_order__cast_data
)

, fact_purchase_order__convert_boolean AS (
SELECT
  *
  , CASE 
      WHEN is_order_finalized_boolean is TRUE THEN 'Order Finalized'
      WHEN is_order_finalized_boolean is FALSE THEN 'Order Not Finalized'
      ELSE 'Invalid'
    END AS is_order_finalized
  , CASE 
      WHEN is_order_finalized_boolean is TRUE THEN 'true'
      WHEN is_order_finalized_boolean is FALSE THEN 'false'
      ELSE 'Invalid'
    END AS is_order_finalized_text
FROM fact_purchase_order__handle_null
) 

SELECT 
   purchase_order_id
  , supplier_id
  , delivery_method_id
  , contact_person_id
  , order_date
  , expected_delivery_date
  , is_order_finalized_text
FROM fact_purchase_order__convert_boolean 