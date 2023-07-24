with customer_categories__source as (
SELECT 
*
FROM `vit-lam-data.wide_world_importers.sales__customer_categories` 
)

, customer_categories__rename_column as(
select
  customer_category_id as customer_category_id
  ,customer_category_name
from customer_categories__source
)

, customer_categories__cast_type as (
select
  cast(customer_category_id as integer) as customer_category_id
  , cast(customer_category_name as string ) as customer_category_name
from customer_categories__rename_column
)


select
  COALESCE( customer_category_id , 0) AS customer_category_id
  ,COALESCE(customer_category_name , "Undefine") AS customer_category_name
from customer_categories__cast_type