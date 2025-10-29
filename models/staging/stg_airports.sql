WITH airports_regions AS (
  SELECT * 
  FROM {{source('flights_data', 'airports')}} AS a
  LEFT JOIN {{source('flights_data', 'regions')}} AS re
    ON a.country = re.country
)
SELECT * FROM airports_regions