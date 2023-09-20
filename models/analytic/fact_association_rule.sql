WITH association_rule__fillter_order_id as(
SELECT
  order_id
  , COUNT(product_id) as num_product
FROM {{ref('fact_sale_order_line')}} 
GROUP BY 1
Having COUNT(product_id) > 1
)

, association_rule__product_vs_order_id as (
SELECT
  order_id
  , product_id
FROM {{ref('fact_sale_order_line')}} 
WHERE order_id in (SELECT order_id FROM association_rule__fillter_order_id)
)

, association_rule__num_order_by_combo as(
  SELECT 
    left_table.product_id as left_product_id
    , right_table.product_id as right_product_id
    , count(left_table.order_id) as num_combo_order
  FROM association_rule__product_vs_order_id as left_table
  LEFT JOIN association_rule__product_vs_order_id as right_table
  ON left_table.order_id = right_table.order_id 
  WHERE left_table.product_id <> right_table.product_id
  GROUP BY 1,2
  )

  , association_rule__num_product AS(
  SELECT 
    product_id
    , count(product_id) as num_product
  FROM {{ref('fact_sale_order_line')}} 
  GROUP BY 1
  )

  , association_rule__num_left_and_right_product as(
  SELECT 
    main.left_product_id
    , main.right_product_id
    , main.num_combo_order*1.0 as num_combo_order
    , (SELECT COUNT(DISTINCT order_id) FROM {{ref('fact_sale_order_line')}})*1.0  as num_total_order
    , left_table.num_product*1.0 as num_left_product
    , right_table.num_product*1.0 as num_right_product
  FROM association_rule__num_order_by_combo as main
  LEFT JOIN association_rule__num_product as left_table
    ON main.left_product_id = left_table.product_id
  LEFT JOIN association_rule__num_product as right_table
    ON main.right_product_id = right_table.product_id
  )

  select 
	left_product_id
	, right_product_id
	, num_combo_order
	, num_left_product
	, num_right_product
	, num_total_order
	, round((num_combo_order/ num_total_order) , 4) as support 
	, round((num_combo_order / num_left_product) ,2) as confidence
	, round((num_combo_order/num_left_product)/(num_right_product/num_total_order),2) as lift
from association_rule__num_left_and_right_product
order by 3 desc





