WITH dim_category AS(
SELECT * 
FROM {{ref('stg_dim_external_categories')}}
)


SELECT * 
  , category_id AS category_level_1_id
  , category_name AS category_level_1_name
  , 0 AS category_level_2_id
  , 'Undefined' AS category_level_2_name
  , 0 AS category_level_3_id
  , 'Undefined' AS category_level_3_name
  , 0 AS category_level_4_id
  , 'Undefined' AS category_level_4_name
FROM dim_category
WHERE 
category_level = 1
UNION ALL
SELECT * 
  , parent_category_id AS category_level_1_id
  , parent_category_name AS category_level_1_name
  , category_id AS category_level_2_id
  , category_name AS category_level_2_name
  , 0 AS category_level_3_id
  , 'Undefined' AS category_level_3_name
  , 0 AS category_level_4_id
  , 'Undefined' AS category_level_4_name
FROM dim_category 
WHERE 
category_level = 2
UNION ALL
SELECT
  level_3.*
  , level_2.parent_category_id AS category_level_1_id
  , level_2.parent_category_name AS category_level_1_name
  , level_3.parent_category_id AS category_level_2_id
  , level_3.parent_category_name AS category_level_2_name
  , level_3.category_id AS category_level_3_id
  , level_3.category_name AS category_level_3_name
  , 0 AS category_level_4_id
  , 'Undefined' AS category_level_4_name
FROM dim_category AS level_3
LEFT JOIN dim_category AS level_2
  ON level_3.parent_category_id = level_2.category_id
WHERE level_3.category_level = 3
UNION ALL
SELECT
  level_4.*
  , level_2.parent_category_id AS category_level_1_id
  , level_2.parent_category_name AS category_level_1_name
  , level_3.parent_category_id AS category_level_2_id
  , level_3.parent_category_name AS category_level_2_name
  , level_4.parent_category_id AS category_level_3_id
  , level_4.parent_category_name AS category_level_3_name
  , level_4.category_id AS category_level_4_id
  , level_4.category_name AS category_level_4_name
FROM dim_category AS level_4
LEFT JOIN dim_category AS level_3
  ON level_4.parent_category_id = level_3.category_id
LEFT JOIN dim_category AS level_2
  ON level_3.parent_category_id = level_2.category_id
WHERE level_4.category_level = 4