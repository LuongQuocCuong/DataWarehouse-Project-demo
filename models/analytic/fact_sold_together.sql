WITH sold_together__source as(
SELECT 
  order_line.order_id
  , product.product_name
  , COUNT(DISTINCT order_line.order_id) OVER() as total_order
  , COUNT(product.product_name) OVER(Partition by order_line.order_id ) as num_product
FROM {{ref('fact_sale_order_line')}} as order_line
LEFT JOIN {{ref('dim_product')}} as product
  USING(product_id) 
)

, sold_together__fillter_num_product AS(
SELECT
  order_id
  , product_name
  , num_product
  , total_order
FROM sold_together__source
WHERE num_product > 2
)

,sold_together__combile_value as(
  SELECT
  order_id
  , num_product
  , total_order
  , STRING_AGG(product_name , " , ") as product_name
FROM sold_together__fillter_num_product
Group by  order_id , num_product, total_order
)

SELECT
  COUNT(order_id) AS total_combo_order
  , total_order
  , round((COUNT(order_id) / total_order),7)  as ratio_purchase
  , num_product
  , product_name
FROM sold_together__combile_value
GROUP BY product_name , num_product, total_order
ORDER BY COUNT(order_id) DESC

