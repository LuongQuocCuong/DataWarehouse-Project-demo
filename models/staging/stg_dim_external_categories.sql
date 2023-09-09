WITH RECURSIVE external_categories__add_level AS (
SELECT
    category_id
    , category_name
    , parent_category_id
    , 1 AS category_level
FROM {{ref("dim_external_categories")}}
WHERE parent_category_id = 0
UNION ALL
SELECT
    child.category_id
    , child.category_name
    , child.parent_category_id
    , parent.category_level + 1
FROM external_categories__add_level AS parent
INNER JOIN {{ref("dim_external_categories")}} AS child
    ON child.parent_category_id = parent.category_id
)

SELECT
*
FROM external_categories__add_level
ORDER BY 1