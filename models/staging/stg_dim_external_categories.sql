WITH external_categories__cast_data AS (
SELECT
  CAST(category_id AS INTEGER) AS category_id
  , CAST (category_name AS STRING) AS category_name
  , CAST (parent_category_id AS INTEGER) AS parent_category_id
  , CAST (category_level AS INTEGER) AS category_level
FROM {{ref("external_categories")}}
)

, external_categories__null_handle AS (
SELECT
  COALESCE(category_id, 0) AS category_id
  , COALESCE(category_name, 'Invalid') AS category_name
  , COALESCE(parent_category_id, 0) AS parent_category_id
  , COALESCE(category_level, 0) AS category_level
FROM external_categories__cast_data
)

SELECT
  dim_category.category_id
  , dim_category.category_name
  , dim_category.parent_category_id
  , COALESCE(dim_parent_category.category_name , "Undefine") AS parent_category_name
  , dim_category.category_level
FROM external_categories__null_handle AS dim_category
LEFT JOIN external_categories__null_handle AS dim_parent_category
  ON dim_category.parent_category_id = dim_parent_category.category_id
-- ORDER BY category_id 