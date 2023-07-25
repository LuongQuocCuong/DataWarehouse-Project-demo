WITH customer_relationship__source AS (
  SELECT
    *
  FROM `vit-lam-data.wide_world_importers.external__customer_membership`
)

, customer_relationship__cast_data AS(
  SELECT
    CAST(customer_id AS INTEGER) AS customer_id
    , CAST(membership AS STRING) AS membership
    , CAST(begin_effective_date AS DATE) AS begin_effective_date
    , CAST(end_effective_date AS DATE) AS end_effective_date
  FROM customer_relationship__source
)

, customer_relationship__add_undefined_record AS (
SELECT 
    customer_id
    , membership
    , begin_effective_date
    , end_effective_date
FROM customer_relationship__cast_data
UNION ALL
SELECT 
    0 AS customer_id
    ,'Undefine' AS membership
    , NULL AS begin_effective_date
    , NULL AS end_effective_date
, UNION ALL
SELECT 
    -1 AS customer_id
    ,'InvaLid' AS membership
    , NULL AS begin_effective_date
    , NULL AS end_effective_date
)

SELECT
    customer_id
    , membership
    , begin_effective_date
    , end_effective_date
FROM customer_relationship__add_undefined_record