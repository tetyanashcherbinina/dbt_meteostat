{{ config(materialized='table') }}
WITH route_statistics AS (
    SELECT
        origin,
        dest,
        COUNT(*) AS total_flights,
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines,
        AVG(actual_elapsed_time)::INTEGER * ('1 second'::INTERVAL) AS avg_actual_elapsed_time,
        AVG(arr_delay)::INTEGER * ('1 second'::INTERVAL) AS avg_arr_delay,
        MAX(arr_delay) AS max_arr_delay,
        MIN(arr_delay) AS min_arr_delay,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted
    FROM {{ ref('prep_flights') }}
    GROUP BY origin, dest
)
SELECT
    r.origin,
    pa.name AS origin_name,
    pa.city AS origin_city,
    pa.country AS origin_country,
    r.dest,
    pd.name AS dest_name,
    pd.city AS dest_city,
    pd.country AS dest_country,
    r.total_flights,
    r.unique_airplanes,
    r.unique_airlines,
    r.avg_actual_elapsed_time,
    r.avg_arr_delay,
    r.max_arr_delay,
    r.min_arr_delay,
    r.total_cancelled,
    r.total_diverted
FROM route_statistics AS r
LEFT JOIN {{ ref('prep_airports') }} AS pa
	ON r.origin = pa.faa
LEFT JOIN {{ ref('prep_airports') }} AS pd
    ON r.dest = pd.faa