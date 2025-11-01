WITH sales AS (
    SELECT * FROM {{ref('prep_sales') }}
)
SELECT 
    order_year,
	order_month,
    category_id,
    category_name,
    SUM(revenue) AS total_revenue,
    COUNT (DISTINCT order_id) AS number_of_orders,
    ROUND (AVG(revenue),2) AS average_revenue
FROM sales
GROUP BY order_year,
	order_month,
	category_id,category_name
ORDER BY order_year,
	order_month