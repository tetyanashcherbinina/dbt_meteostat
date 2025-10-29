{{ config(materialized='view') }}
SELECT * 
FROM {{ source('flights_data', 'flights') }}
WHERE flight_date BETWEEN '2024-01-01' AND '2024-01-31'
