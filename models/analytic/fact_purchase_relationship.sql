WITH relationship_category__source AS (
  SELECT 
    order_line.order_id,
    product.category_name
  FROM {{ref('fact_sale_order_line')}} AS order_line
  LEFT JOIN {{ref('dim_product')}} AS product
    USING (product_id)
),

relationship_category__add_is_purchase_column AS (
  SELECT DISTINCT
    order_id,
    category_name,
    1 AS is_purchase
  FROM relationship_category__source
),

relationship_category__add_num_category AS (
  SELECT
    order_id,
    category_name,
    is_purchase,
    SUM(is_purchase) OVER (PARTITION BY order_id) AS num_category
  FROM relationship_category__add_is_purchase_column
  ORDER BY 4 DESC
),

relationship_category__filter_num_category AS (
  SELECT * 
  FROM relationship_category__add_num_category
  WHERE num_category > 1
),

relationship_category__pivot AS (
  SELECT 
    *
  FROM relationship_category__filter_num_category
  PIVOT (
    COUNT(is_purchase) FOR category_name IN (
      'Wheatmeal Cookies' AS Wheatmeal_Cookies,
      'Coffee' AS Coffee,
      'Candy' AS Candy,
      'Biscuits' AS Biscuits,
      'Cookies Biscuits' AS Cookies_Biscuits,
      'Tea' AS Tea,
      'Juice' AS Juice,
      'Original Cookies' AS Original_Cookies,
      'Butter Biscuits' AS Butter_Biscuits,
      'Light Cookies' AS Light_Cookies,
      'Beer' AS Beer,
      'Chocolate' AS Chocolate,
      'Cheese Biscuits' AS Cheese_Biscuits
    )
  )
)

SELECT
  COUNT(order_id) AS order_num 
  , num_category
  , Wheatmeal_Cookies
  , Coffee
  , Candy
  , Biscuits
  , Cookies_Biscuits
  , Tea
  , Juice
  , Original_Cookies
  , Butter_Biscuits
  , Light_Cookies
  , Beer
  , Chocolate
  , Cheese_Biscuits
FROM relationship_category__pivot
GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15
