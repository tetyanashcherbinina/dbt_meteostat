with source_data AS (
    select *
    FROM {{ source('northwind_data', 'orders') }}
)
select
    order_id,
    customer_id,
    employee_id,
    order_date_tmp::date AS order_date, 
    required_date_tmp::date AS required_date, 
    shipped_date_tmp::date AS shipped_date,
    ship_via AS shipper_id,
    ship_name,
    ship_address,
    ship_city,
    ship_region,
    ship_postal_code,
    ship_country
from source_data