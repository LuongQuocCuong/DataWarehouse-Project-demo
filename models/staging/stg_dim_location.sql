WITH location__join AS(
SELECT
  city.city_id as city_id
  , city.city_name as city_name
  , province.province_id
  , province.province_name
  , country.country_id
  , country.country_name
FROM {{ref('stg_dim_location_city')}} as city
LEFT JOIN {{ref('stg_dim_location_province')}} as province
  ON city.province_id = province.province_id
LEFT JOIN {{ref('stg_dim_location_country')}} as country
  ON province.country_id = country.country_id
)

, Location__add_invalid_column AS(
SELECT
     city_id
    , city_name
    , province_id
    , province_name
    , country_id
    , country_name
FROM location__join
UNION ALL
SELECT
    0 AS city_id
    , 'Undefine' AS city_name
    , 0 AS province_id
    , 'Undefine' AS province_name
    , 0 AS country_id
    , 'Undefine' AS country_name
, UNION ALL
SELECT
    -1 AS city_id
    , 'Invalid' AS city_name
    , -1 AS province_id
    , 'Invalid' AS province_name
    , -1 AS country_id
    , 'Invalid' AS country_name
)

SELECT
     city_id
    , city_name
    , province_id
    , province_name
    , country_id
    , country_name
FROM Location__add_invalid_column