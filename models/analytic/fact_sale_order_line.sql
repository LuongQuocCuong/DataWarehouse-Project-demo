WITH fact_sale__soure AS (
SELECT * 
from `vit-lam-data.wide_world_importers.sales__order_lines` 
)

, fact_sale_rename_column AS (
SELECT
	order_line_id AS order_line_id
  , order_id AS order_id
  , stock_item_id AS product_id
  , package_type_id AS package_type_id
  , quantity AS quantity
  , unit_price 
  , picked_quantity
  , tax_rate
  , picking_completed_when
FROM fact_sale__soure
)

, fact_sale_cast_data AS (
SELECT
  CAST (order_line_id AS integer) AS order_line_id
  , CAST (order_id AS integer) AS order_id
  , CAST (product_id AS integer) AS product_id
  , CAST (package_type_id AS integer) AS package_type_id
  , CAST (quantity AS integer) AS quantity
  , CAST (unit_price AS numeric) AS unit_price
  , CAST (picked_quantity AS integer) AS picked_quantity
  , CAST (tax_rate AS numeric) AS tax_rate
  , CAST (picking_completed_when AS date) AS picking_completed_when
from fact_sale_rename_column
)

, fact_sale__undefined_handle AS (
SELECT
  COALESCE (order_line_id, 0) AS order_line_id
  , COALESCE (order_id, 0) AS order_id
  , COALESCE (product_id, 0) AS product_id
  , COALESCE (package_type_id, 0) AS package_type_id
  , COALESCE (quantity, 0) AS quantity
  , COALESCE (picked_quantity, 0) AS picked_quantity
  , COALESCE (tax_rate, 0) AS tax_rate
  , COALESCE (unit_price, 0) AS unit_price
  , picking_completed_when
FROM fact_sale_cast_data
)

SELECT 
   fact_line.order_line_id
, COALESCE (fact_line.order_id,0) AS order_id
, COALESCE (fact_line.product_id,0) AS product_id
, COALESCE (fact_line.package_type_id,0) AS package_type_id
, COALESCE (stg_sale_order.customer_id , 0) AS customer_id
, COALESCE (stg_sale_order.picked_by_person_id , 0) AS picked_by_person_id
, COALESCE (stg_sale_order.sales_person_id , 0) AS sales_person_id
, COALESCE (stg_sale_order.contact_person_id ,0) AS contact_person_id
, fact_line.picking_completed_when AS line_picking_completed_when
, stg_sale_order.picking_completed_when AS order_picking_completed_when
, COALESCE (FARM_FINGERPRINT(CONCAT(stg_sale_order.is_undersupply_back_ordered_boolean,"-",fact_line.package_type_id)),0) AS sales_order_line_indicator_id
, stg_sale_order.order_date
, stg_sale_order.expected_delivery_date
, fact_line.picked_quantity
, fact_line.quantity
, fact_line.unit_price
, fact_line.tax_rate/100 AS tax_rate
, fact_line.quantity * fact_line.unit_price AS gross_amount
, fact_line.quantity * fact_line.unit_price * fact_line.tax_rate/100 AS tax_amount
, (fact_line.quantity * fact_line.unit_price) - (fact_line.quantity * fact_line.unit_price * fact_line.tax_rate/100) AS net_amount
FROM fact_sale__undefined_handle AS fact_line
LEFT JOIN {{ref('stg_fact_sale_order')}} AS stg_sale_order
  ON fact_line.order_id = stg_sale_order.order_id
