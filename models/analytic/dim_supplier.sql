WITH dim_supplier__source AS (
SELECT 
  *
FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_column AS (
SELECT 
   supplier_id AS supplier_id
  ,supplier_name	AS supplier_name
  ,supplier_category_id AS supplier_category_id
  ,delivery_city_id AS supplier_delivery_city_id
FROM dim_supplier__source
)

, dim_supplier__join AS(
SELECT 
  dim_supllier.supplier_id
  , dim_supllier.supplier_name AS supplier_name
  , dim_supllier.supplier_category_id AS supplier_category_id
  , supplier_category.supplier_category_name AS supplier_category_name
  , dim_supllier.supplier_delivery_city_id AS supplier_city_id
  , location.city_name AS supplier_city_name
  , location.province_id AS supplier_province_id
  , location.province_name AS supplier_province_name
  , location.country_id AS supplier_country_id
  , location.country_name AS supplier_country_name
FROM dim_supplier__rename_column AS dim_supllier
LEFT JOIN {{ref('stg_dim_supplier_category')}} AS supplier_category
  ON dim_supllier.supplier_category_id = supplier_category.supplier_category_id
LEFT JOIN {{ref('stg_dim_location')}} AS location
  ON location.city_id = dim_supllier.supplier_delivery_city_id 
)


, dim_supplier__cast_data AS (
SELECT
  CAST (supplier_id AS integer) AS supplier_id
  , CAST (supplier_name AS string) AS supplier_name
  , CAST (supplier_category_id AS integer) AS supplier_category_id
  , CAST (supplier_delivery_city_id AS integer) AS supplier_delivery_city_id
FROM dim_supplier__rename_column
)

, dim_supplier__add_undefined_record AS (
SELECT
  supplier_id
  ,  supplier_name
  , supplier_category_id
  , supplier_category_name
  , supplier_city_id
  , supplier_city_name
  , supplier_province_id
  , supplier_province_name
  , supplier_country_id
  , supplier_country_name
FROM dim_supplier__join

UNION ALL 
  SELECT
    0 AS supplier_id
    , 'Undefined' AS supplier_name
    , 0 AS supplier_category_id
    , 'Undefined' AS supplier_category_name
    , 0 AS supplier_city_id
    , 'Undefined' AS supplier_city_name
    , 0 AS supplier_province_id
    , 'Undefined' AS supplier_province_name
    , 0 AS supplier_country_id
    , 'Undefined' AS supplier_country_name

, UNION ALL 
  SELECT
    -1 AS supplier_id
    , 'Invalid' AS supplier_name
    , -1 AS supplier_category_id
    , 'Invalid' AS supplier_category_name
    , -1 AS supplier_city_id
    , 'Invalid' AS supplier_city_name
    , -1 AS supplier_province_id
    , 'Invalid' AS supplier_province_name
    , -1 AS supplier_country_id
    , 'Invalid' AS supplier_country_name
)

SELECT  
   supplier_id
  , supplier_name
  , supplier_category_id
  , supplier_city_id
  , supplier_city_name
  , supplier_province_id
  , supplier_province_name
  , supplier_country_id
  , supplier_country_name 
FROM dim_supplier__add_undefined_record
