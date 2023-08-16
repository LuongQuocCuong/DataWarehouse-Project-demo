WITH churn_customer__source AS (
SELECT
  customer_id
  , order_date
FROM {{ref("fact_sale_order_line")}}
)

, churn_customer__first_date_purchase AS(
  SELECT
    customer_id
    , MIN(order_date) AS first_date_purchase
  FROM churn_customer__source
  GRoup BY 1
)

, churn_customer__join AS (
SELECT
  churn_customer__source.customer_id
  , churn_customer__source.order_date
  , churn_customer__first_date_purchase.first_date_purchase
  , DATE_DIFF(churn_customer__source.order_date , churn_customer__first_date_purchase.first_date_purchase, DAY ) AS distance_day
FROM churn_customer__source 
LEFT JOIN churn_customer__first_date_purchase
USING (customer_id)
GROUP BY 1, 2,3
ORDER BY 1,2,3
)

SELECT
  customer_id
  , order_date
  , first_date_purchase
  , CASE  
      WHEN distance_day = 0 THEN 'New'
      ELSE 'Return'
    END AS customer_segment
FROM churn_customer__join