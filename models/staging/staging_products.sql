with source_data AS (
    select *
    from {{ source('northwind_data', 'products') }}
)
select
    product_id,
    product_name,
    supplier_id,
    category_id,
    unit_price::numeric as unit_price,
    units_in_stock::numeric as units_in_stock,
    units_on_order::numeric as units_on_order
from source_data