with buying_group__source as (
SELECT 
*
FROM `vit-lam-data.wide_world_importers.sales__buying_groups`
)

, buying_group__source__rename_column as(
select
  buying_group_id as buying_group_id
  ,buying_group_name
from buying_group__source
)

, buying_group__cast_type as (
select
  cast(buying_group_id as integer) as buying_group_id
  , cast(buying_group_name as string ) as buying_group_name
from buying_group__source__rename_column
)


select
  COALESCE(buying_group_id , 0) AS buying_group_id
  , COALESCE(buying_group_name, "Undefine") AS buying_group_name
from buying_group__cast_type