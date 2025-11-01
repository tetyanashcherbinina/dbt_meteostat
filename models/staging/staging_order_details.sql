with source_data AS (
    select *
    from {{ source('northwind_data', 'order_details') }}
)
select
    order_id,
    product_id,
    unit_price::numeric AS unit_price,
    quantity::int AS quantity,
    discount::numeric AS discount
from source_data