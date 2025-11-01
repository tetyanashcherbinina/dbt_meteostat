WITH orders AS (
    SELECT 
    customer_id,
    order_id,
    order_date
    FROM {{ ref('staging_orders') }}
),
order_details AS (
    SELECT *
    FROM {{ ref('staging_order_details') }}
),
products AS (
    select 
    product_id,
    product_name,
    category_id
    FROM {{ ref('staging_products') }}
),
categories AS (
    SELECT 
    category_id,
    category_name
    FROM {{ ref('staging_categories') }}
)
SELECT 
	o.customer_id,
    o.order_id,
    o.order_date,
    EXTRACT(YEAR FROM o.order_date) AS order_year,
	EXTRACT(MONTH FROM o.order_date) AS order_month,
    d.product_id,
    p.product_name,
    c.category_id,
    c.category_name,
    d.unit_price,
    d.quantity,
    d.discount,
	d.unit_price * d.quantity * (1 - d.discount) AS revenue
FROM orders AS o
JOIN order_details AS d 
USING (order_id)
JOIN products AS p
USING (product_id)
LEFT JOIN  categories AS c 
USING (category_id)	