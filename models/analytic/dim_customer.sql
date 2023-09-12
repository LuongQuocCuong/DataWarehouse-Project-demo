with
    dim_customer__source as (
        select * from `vit-lam-data.wide_world_importers.sales__customers`
    ),
    dim_customer__rename_coulumn as (
        select
            customer_id,
            customer_name as customer_name,
            is_statement_sent,
            is_on_credit_hold,
            credit_limit,
            standard_discount_percentage,
            payment_days,
            account_opened_date,
            customer_category_id,
            buying_group_id,
            delivery_method_id,
            delivery_city_id,
            postal_city_id,
            primary_contact_person_id,
            alternate_contact_person_id,
            bill_to_customer_id
        from dim_customer__source
    ),
    dim_customer__cast_data as (
        select
            cast(customer_id as integer) as customer_id,
            cast(customer_name as string) as customer_name,
            cast(is_statement_sent as boolean) as is_statement_sent_boolean,
            cast(is_on_credit_hold as boolean) as is_on_credit_hold_boolean,
            cast(credit_limit as numeric) as credit_limit,
            cast(
                standard_discount_percentage as numeric
            ) as standard_discount_percentage,
            cast(payment_days as integer) as payment_days,
            cast(account_opened_date as date) as account_opened_date,
            cast(customer_category_id as integer) as customer_category_id,
            cast(buying_group_id as integer) as buying_group_id,
            cast(delivery_method_id as integer) as delivery_method_id,
            cast(delivery_city_id as integer) as delivery_city_id,
            cast(postal_city_id as integer) as postal_city_id,
            cast(primary_contact_person_id as integer) as primary_contact_person_id,
            cast(alternate_contact_person_id as integer) as alternate_contact_person_id,
            cast(bill_to_customer_id as integer) as bill_to_customer_id
        from dim_customer__rename_coulumn
    ),
    dim_customer__convert_data as (
        select
            *,
            case
                when is_on_credit_hold_boolean is true
                then 'On Hold Credit'
                when is_on_credit_hold_boolean is false
                then 'Not On Hold Credit'
                when is_on_credit_hold_boolean is null
                then 'Undefined'
                else 'invalid'
            end as is_on_credit_hold,
            case
                when is_statement_sent_boolean is true
                then 'Statement Sent'
                when is_statement_sent_boolean is false
                then 'Not Statement Sent'
                when is_statement_sent_boolean is null
                then 'Undefined'
                else 'invalid'
            end as is_statement_sent
        from dim_customer__cast_data
    ),
    dim_customer__handle_null as (
        select
            customer_id,
            coalesce(customer_name, 'Undefined') as customer_name,
            is_statement_sent,
            is_on_credit_hold,
            coalesce(credit_limit, 0) as credit_limit,
            coalesce(standard_discount_percentage, 0) as standard_discount_percentage,
            coalesce(payment_days, 0) as payment_days,
            account_opened_date,
            coalesce(customer_category_id, 0) as customer_category_id,
            coalesce(delivery_city_id, 0) as delivery_city_id,
            coalesce(buying_group_id, 0) as buying_group_id,
            coalesce(delivery_method_id, 0) as delivery_method_id,
            coalesce(postal_city_id, 0) as postal_city_id,
            coalesce(primary_contact_person_id, 0) as primary_contact_person_id,
            coalesce(alternate_contact_person_id, 0) as alternate_contact_person_id,
            coalesce(bill_to_customer_id, 0) as bill_to_customer_id
        from dim_customer__convert_data
    ),
    dim_customer__add_undefined_record as (
        select
            customer_id,
            customer_name,
            is_statement_sent,
            is_on_credit_hold,
            credit_limit,
            standard_discount_percentage,
            payment_days,
            account_opened_date,
            customer_category_id,
            delivery_city_id,
            buying_group_id,
            delivery_method_id,
            postal_city_id,
            primary_contact_person_id,
            alternate_contact_person_id,
            bill_to_customer_id
        from dim_customer__handle_null
        union all
        select
            0 as customer_id,
            'Undefined' as customer_name,
            'Undefined' as is_statement_sent,
            'Undefined' as is_on_credit_hold,
            0 as credit_limit,
            0 as standard_discount_percentage,
            0 as payment_days,
            null as account_opened_date,
            0 as customer_category_id,
            0 as delivery_city_id,
            0 as buying_group_id,
            0 as delivery_method_id,
            0 as postal_city_id,
            0 as primary_contact_person_id,
            0 as alternate_contact_person_id,
            0 as bill_to_customer_id,
        union all
        select
            -1 as customer_id,
            'Invalid' as customer_name,
            'Invalid' as is_statement_sent,
            'Invalid' as is_on_credit_hold,
            -1 as credit_limit,
            -1 as standard_discount_percentage,
            -1 as payment_days,
            null as account_opened_date,
            -1 as customer_category_id,
            -1 as delivery_city_id,
            -1 as buying_group_id,
            -1 as delivery_method_id,
            -1 as postal_city_id,
            -1 as primary_contact_person_id,
            -1 as alternate_contact_person_id,
            -1 as bill_to_customer_id
    )

select
    dim_customer.customer_id,
    dim_customer.customer_name,
    dim_customer.is_statement_sent,
    dim_customer.is_on_credit_hold,
    dim_customer.credit_limit,
    dim_customer.standard_discount_percentage,
    dim_customer.payment_days,
    dim_customer.account_opened_date,
    dim_customer.customer_category_id,
    coalesce(
        customer_category.customer_category_name, 'Invalid'
    ) as customer_category_name,
    dim_customer.buying_group_id,
    coalesce(buying_group.buying_group_name, 'Invalid') as buying_group_name,
    dim_customer.delivery_method_id,
    coalesce(delivery_method.delivery_method_name, 'Invalid') as delivery_method_name,
    dim_customer.delivery_city_id,
    coalesce(location.city_name, 'Invalid') as delivery_city_name,
    coalesce(location.province_id, 0) as delivery_province_id,
    coalesce(location.province_name, 'Invalid') as delivery_province_name,
    coalesce(location.country_id, 0) as delivery_country_id,
    coalesce(location.country_name, 'Invalid') as delivery_country_name,
    dim_customer.postal_city_id,
    coalesce(postal_location.city_name, 'Invalid') as postal_city_name,
    coalesce(postal_location.province_id, 0) as postal_province_id,
    coalesce(postal_location.province_name, 'Invalid') as postal_province_name,
    coalesce(postal_location.country_id, 0) as postal_country_id,
    coalesce(postal_location.country_name, 'Invalid') as postal_country_name,
    dim_customer.primary_contact_person_id,
    coalesce(
        dim_person__primary_contact.full_name, 'Invalid'
    ) as primary_contact_person_name,
    dim_customer.alternate_contact_person_id,
    coalesce(
        dim_person__alternate_contact.full_name, 'Invalid'
    ) as alternate_contact_person_name,
    dim_customer.customer_id as bill_to_customer_id,
    coalesce(bill_to_customer.customer_name, 'Invalid') as bill_to_customer_name
from dim_customer__add_undefined_record as dim_customer
left join
    {{ ref("stg_dim_customer_categories") }} as customer_category
    on customer_category.customer_category_id = dim_customer.customer_category_id
left join
    {{ ref("stg_dim_buying_group") }} as buying_group
    on dim_customer.buying_group_id = buying_group.buying_group_id
left join
    {{ ref("stg_dim_delivery_method") }} as delivery_method
    on delivery_method.delivery_method_id = dim_customer.delivery_method_id
left join
    {{ ref("stg_dim_location") }} as location
    on location.city_id = dim_customer.delivery_city_id
left join
    {{ ref("stg_dim_location") }} as postal_location
    on postal_location.city_id = dim_customer.postal_city_id
left join
    {{ ref("dim_person") }} as dim_person__primary_contact
    on dim_person__primary_contact.person_id = dim_customer.primary_contact_person_id
left join
    {{ ref("dim_person") }} as dim_person__alternate_contact
    on dim_person__alternate_contact.person_id
    = dim_customer.alternate_contact_person_id
left join
    dim_customer__handle_null as bill_to_customer
    on bill_to_customer.customer_id = dim_customer.customer_id
