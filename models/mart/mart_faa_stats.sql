{{ config(materialized='table') }}
WITH departures AS (
    SELECT
        origin AS airport,
        COUNT(DISTINCT dest) AS unique_destinations,
        COUNT(*) AS total_departures,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_departures,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS diverted_departures,
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS completed_departures,
        COUNT(DISTINCT tail_number) AS unique_airplanes_dep,
        COUNT(DISTINCT airline) AS unique_airlines_dep
    FROM {{ ref('prep_flights') }}
    GROUP BY origin
),
arrivals AS (
    SELECT
        dest AS airport,
        COUNT(DISTINCT origin) AS unique_origions,
        COUNT(*) AS total_arrivals,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_arrivals,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS diverted_arrivals,
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS completed_arrivals,
        COUNT(DISTINCT tail_number) AS unique_airplanes_arr,
        COUNT(DISTINCT airline) AS unique_airlines_arr
    FROM  {{ ref('prep_flights') }}
    GROUP BY dest
),
resulted AS (
    SELECT
        d.airport,
        d.unique_destinations,
        a.unique_origions,
        (d.total_departures + a.total_arrivals) AS planned_flights,
        (d.cancelled_departures + a.cancelled_arrivals) AS total_cancelled,
        (d.diverted_departures + a.diverted_arrivals) AS total_diverted,
        (d.completed_departures + a.completed_arrivals) AS total_occured,
        (d.unique_airplanes_dep + a.unique_airplanes_arr) AS unique_airplanes,
        (d.unique_airlines_dep + a.unique_airlines_arr) AS unique_airlines
    FROM departures AS d
    FULL JOIN arrivals AS a 
	ON d.airport = a.airport
)
SELECT
    pa.name,
    pa.city,
    pa.country,
    r.*
FROM resulted AS r
LEFT JOIN  {{ ref('prep_airports') }} AS pa
ON r.airport = pa.faa