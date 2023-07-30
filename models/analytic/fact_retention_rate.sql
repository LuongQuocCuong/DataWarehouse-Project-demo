WITH retention_rate AS(
  SELECT
  customer_id
  , order_date
  FROM `project-demo-393802`.`data_warehouse`.`fact_sale_order_line`
  --WHERE order_date = PARSE_DATE('%Y-%m-%d', '2014-01-01')
)

, retention_rate__source AS (
  SELECT 
     customer_id
    , DATE_TRUNC (order_date, MONTH) AS order_date
  FROM retention_rate
)

, retention_rate__first_month_purchase AS ( 
  SELECT 
     customer_id
    , MIN (order_date) AS first_month_purchase
  FROM retention_rate__source
  GROUP BY 1
)

, retention_rate__retention_month AS (
  SELECT
      customer_id
      , order_date AS retention_month
  FROM retention_rate__source 
  GROUP BY 1, 2
)

, retention_rate__join AS(
  SELECT
     first_month.customer_id
     , first_month.first_month_purchase
     , retention_month.retention_month
  FROM retention_rate__first_month_purchase AS first_month
  LEFT JOIN retention_rate__retention_month AS retention_month
  USING (customer_id)
  ORDER BY 1,2,3
)

, retention_rate__new_customner_coulum AS (
  SELECT
     first_month_purchase
    , COUNT(customer_id) AS new_customer
  FROM retention_rate__first_month_purchase
  GROUP BY 1
)

, retention_rate__retention_customer_coulum AS(
  SELECT 
    first_month_purchase
    , retention_month
    , COUNT(DISTINCT(customer_id)) AS retention_customer
  FROM retention_rate__join
  GROUP BY 1,2
)

SELECT 
    EXTRACT(YEAR from first_month_purchase) AS year_start
    , EXTRACT(YEAR from retention_month) AS year_end
    ,retention_rate__retention_customer_coulum.first_month_purchase AS first_month_purchase
  ,  retention_rate__retention_customer_coulum.retention_month AS retention_month
  , DATE_DIFF(retention_rate__retention_customer_coulum.retention_month ,retention_rate__retention_customer_coulum.first_month_purchase, MONTH ) + 1 AS month_index
  , retention_rate__retention_customer_coulum.retention_customer
  , retention_rate__retention_customer_coulum.retention_customer / retention_rate__new_customner_coulum.new_customer 
FROM retention_rate__retention_customer_coulum
LEFT JOIN retention_rate__new_customner_coulum 
USING (first_month_purchase)
ORDER BY 1,2