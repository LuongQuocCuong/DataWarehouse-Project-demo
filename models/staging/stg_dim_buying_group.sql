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

, buying_group__add_undefined_record AS(
select
    buying_group_id
  , buying_group_name
from buying_group__cast_type
UNION ALL
SELECT
    0 AS buying_group_id
    ,'Undefine' AS buying_group_name
, UNION ALL
SELECT
    -1 AS buying_group_id
    ,'Invalid' AS buying_group_name
)

SELECT
    buying_group_id
    , buying_group_name
FROM buying_group__add_undefined_record
