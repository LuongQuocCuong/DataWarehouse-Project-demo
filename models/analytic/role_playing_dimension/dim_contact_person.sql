SELECT
  person_id as contact_person_id
  , full_name as contact_person_name
  , is_employee	
  , is_sales_person
FROM {{ref('dim_person')}}
