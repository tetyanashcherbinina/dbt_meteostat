{{ config(materialized='table') }}
WITH flights_numeric AS (
    SELECT
        flight_date,
        airline,
        tail_number,
        flight_number,
        origin,
        dest,
        TO_CHAR(dep_time, 'fm0000')::TIME AS dep_time,
        TO_CHAR(sched_dep_time, 'fm0000')::TIME AS sched_dep_time,
        dep_delay,
        (dep_delay * '1 minute'::interval) AS dep_delay_interval,
        TO_CHAR(arr_time, 'fm0000')::TIME AS arr_time,
        TO_CHAR(sched_arr_time, 'fm0000')::TIME AS sched_arr_time,
        arr_delay,
        (arr_delay * '1 minute'::interval) AS arr_delay_interval,
        air_time,
        (air_time * '1 minute'::interval) AS air_time_interval,
        actual_elapsed_time,
        (actual_elapsed_time * '1 minute'::interval) AS actual_elapsed_time_interval,
        (distance / 0.621371) AS distance_km,
        cancelled,
        diverted
    FROM {{ ref('stg_flights_one_month') }}
)
SELECT *
FROM flights_numeric
