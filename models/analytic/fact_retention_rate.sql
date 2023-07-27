WITH retention_rate__source AS (
  SELECT 
     customer_id
    , order_date
  FROM `project-demo-393802.data_warehouse.fact_sale_order_line`
)

, retention_rate__first_month_purchase AS (
  SELECT 
     customer_id
    , DATE_TRUNC(order_date , MONTH) AS first_month_purchase
  FROM retention_rate__source
)

, retention_rate__retention_month AS (
  SELECT
      customer_id
      , first_month_purchase AS retention_month
  FROM retention_rate__first_month_purchase 
  GROUP BY 1,2
)

, retention_rate__join AS(
  SELECT
     first_month.customer_id
     , first_month.first_month_purchase
     , retention_month.retention_month
  FROM retention_rate__first_month_purchase AS first_month
  LEFT JOIN retention_rate__retention_month AS retention_month
  USING (customer_id)
)

, retention_rate__new_customner_coulum AS (
  SELECT
     first_month_purchase
    , COUNT (customer_id) AS new_customer
  FROM retention_rate__first_month_purchase
  GROUP BY 1
)

, retention_rate__retention_customer_coulum AS(
  SELECT 
    first_month_purchase
    , retention_month
    , COUNT(customer_id) AS retention_customer
  FROM retention_rate__join
  GROUP BY 1,2
)

SELECT 
  retention_rate__retention_customer_coulum.first_month_purchase AS first_month_purchase
  ,  retention_rate__retention_customer_coulum.retention_month AS retention_month
  , retention_rate__new_customner_coulum.new_customer
  , retention_rate__retention_customer_coulum.retention_customer
FROM retention_rate__retention_customer_coulum
LEFT JOIN retention_rate__new_customner_coulum 
USING (first_month_purchase)
ORDER BY 1,2