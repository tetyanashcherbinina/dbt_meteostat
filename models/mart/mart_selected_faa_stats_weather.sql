{{ config(materialized='table') }}
WITH departures AS (
    SELECT
        origin AS airport,
        flight_date,
        COUNT(DISTINCT dest) AS unique_destinations,
        COUNT(*) AS total_departures,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_departures,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS diverted_departures,
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS completed_departures,
        COUNT(DISTINCT tail_number) AS unique_airplanes_dep,
        COUNT(DISTINCT airline) AS unique_airlines_dep
    FROM {{ ref('prep_flights') }}
    WHERE origin IN (SELECT DISTINCT airport_code FROM {{ ref('prep_weather_daily') }})
    GROUP BY origin,flight_date
),
arrivals AS (
    SELECT
        dest AS airport,
        flight_date,
        COUNT(DISTINCT origin) AS unique_origions,
        COUNT(*) AS total_arrivals,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_arrivals,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS diverted_arrivals,
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS completed_arrivals,
        COUNT(DISTINCT tail_number) AS unique_airplanes_arr,
        COUNT(DISTINCT airline) AS unique_airlines_arr
    FROM {{ ref('prep_flights') }}
    WHERE dest IN (SELECT DISTINCT airport_code FROM {{ ref('prep_weather_daily') }})
    GROUP BY dest,flight_date
),
resulted AS (
    SELECT
    	flight_date,
        airport,
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
	USING (flight_date, airport)-- USING automatically matches columns with the same name and returns only one copy
)
SELECT r.* 
		,pw.min_temp_c
		,pw.max_temp_c
		,pw.precipitation_mm
		,pw.max_snow_mm
		,pw.avg_wind_direction
		,pw.avg_wind_speed
		,pw.avg_peakgust
FROM resulted  AS r
LEFT JOIN {{ ref('prep_weather_daily') }} AS pw
ON r.airport = pw.airport_code AND r.flight_date = pw.date