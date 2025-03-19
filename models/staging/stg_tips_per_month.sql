{{ config(materialized='view') }}

SELECT
    taxi_id,
    toStartOfMonth(trip_start_timestamp) AS year_month,
    SUM(tips) AS tips_sum
FROM {{ source('taxi', 'trips_chicago') }}
WHERE trip_start_timestamp IS NOT NULL
GROUP BY taxi_id, year_month
