WITH dim_category_ModelHierarchy AS(
SELECT 
*
FROM {{ref("dim_category_ModelHierarchy")}}
)

, dim_category__map_bridge AS (
SELECT 
   category_level_1_id AS parent_category_id
  , category_level_1_name AS parent_category_name
  ,  category_id AS child_category_id
  , category_name AS child_category_name
FROM dim_category_ModelHierarchy 
UNION ALL
SELECT 
   category_level_2_id AS parent_category_id
  , category_level_2_name AS parent_category_name
  , category_id AS child_category_id
  , category_name AS child_category_name
FROM dim_category_ModelHierarchy 
WHERE category_level_2_id <> 0
UNION ALL
SELECT 
   category_level_3_id AS parent_category_id
  , category_level_3_name AS parent_category_name
  , category_id AS child_category_id
  , category_name AS child_category_name
FROM dim_category_ModelHierarchy 
WHERE category_level_3_id <> 0
UNION ALL
SELECT 
   category_level_4_id AS parent_category_id
  , category_level_4_name AS parent_category_name
  , category_id AS child_category_id
  , category_name AS child_category_name
FROM dim_category_ModelHierarchy 
WHERE category_level_4_id <> 0
ORDER BY 1
)

, dim_category__add_undefined_record AS (
    SELECT
        parent_category_id
        , parent_category_name
        , child_category_id
        , child_category_name
    FROM dim_category__map_bridge
UNION ALL
SELECT
    0 AS parent_category_id
    , "Undefined" AS parent_category_name
    , 0 AS child_category_id
    , "Undefined" AS child_category_name
, UNION ALL
SELECT
    -1 AS parent_category_id
    , "Invalid" AS parent_category_name
    , -1 AS child_category_id
    , "Invalid" AS child_category_name
)

SELECT
    dim.parent_category_id
    , dim.parent_category_name
    , dim.child_category_id
    , dim.child_category_name
    , COALESCE(fact.category_level,0) AS category_level
FROM dim_category__add_undefined_record AS dim
LEFT JOIN {{ref("dim_category_ModelHierarchy")}} AS fact
ON dim.parent_category_id = fact.category_id
ORDER BY 1