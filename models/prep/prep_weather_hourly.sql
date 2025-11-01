{{ config(materialized='table') }}
WITH weather_numeric AS (
    SELECT
        *,
        timestamp::DATE AS date,
        timestamp::TIME AS time,
        TO_CHAR(timestamp, 'HH24:MI') AS hour,
        TO_CHAR(timestamp, 'FMMonth') AS month_name,
        TO_CHAR(timestamp, 'FMDay') AS weekday,
        DATE_PART('day', timestamp) AS date_day,
        DATE_PART('month', timestamp) AS date_month,
        DATE_PART('year', timestamp) AS date_year,
        DATE_PART('week', timestamp) AS cw   
    FROM {{ ref('stg_weather_hourly') }}
)
SELECT
    *,
    CASE
        WHEN time BETWEEN '00:00' AND '05:59' THEN 'night'
        WHEN time BETWEEN '06:00' AND '17:59' THEN 'day'
        WHEN time BETWEEN '18:00' AND '23:59' THEN 'evening'
    END AS day_part
FROM weather_numeric