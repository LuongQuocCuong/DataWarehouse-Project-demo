WITH rfm__source AS (
SELECT
    customer_id
    , DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) AS  recency
    , COUNT(order_id) AS frequency
    , SUM(gross_amount) AS monetary
FROM {{ref("fact_sale_order_line")}}
GROUP BY 1
)

, rfm__percent_rank AS(
  SELECT
    customer_id
    , PERCENT_RANK() OVER (ORDER BY recency DESC) AS recency
    , PERCENT_RANK() OVER (ORDER BY frequency DESC) AS frequency
    , PERCENT_RANK() OVER (ORDER BY monetary DESC) AS monetary
  FROM rfm__source
)

, rfm__percent_add_segment AS(
SELECT
  customer_id
  , CASE
    WHEN recency BETWEEN 0 AND 0.2 THEN '5'
    WHEN recency BETWEEN 0.2 AND 0.4 THEN '4'
    WHEN recency BETWEEN 0.4 AND 0.6 THEN '3'
    WHEN recency BETWEEN 0.6 AND 0.8 THEN '2'
    ELSE '1' END AS recency
  ,   CASE
    WHEN frequency BETWEEN 0 AND 0.2 THEN '1'
    WHEN frequency BETWEEN 0.2 AND 0.4 THEN '2'
    WHEN frequency BETWEEN 0.4 AND 0.6 THEN '3'
    WHEN frequency BETWEEN 0.6 AND 0.8 THEN '4'
    ELSE '5' END AS frequency
  ,   CASE
    WHEN monetary BETWEEN 0 AND 0.2 THEN '1'
    WHEN monetary BETWEEN 0.2 AND 0.4 THEN '2'
    WHEN monetary BETWEEN 0.4 AND 0.6 THEN '3'
    WHEN monetary BETWEEN 0.6 AND 0.8 THEN '4'
    ELSE '5' END AS monetary
FROM rfm__percent_rank
)

, rfm__add_gross_amount_column AS (
    SELECT 
        customer_id
        , SUM(gross_amount) AS total_gross_amount
    FROM {{ref("fact_sale_order_line")}}
    GROUP BY 1
) 

SELECT
  *
  , CONCAT(recency, frequency, monetary) AS rfm_score
  , rfm__add_gross_amount_column.total_gross_amount
FROM rfm__percent_add_segment
LEFT JOIN rfm__add_gross_amount_column
    USING(customer_id)