{{ config(materialized='table') }}

WITH workouts_per_month_per_year AS (
    SELECT
        CAST(year AS SMALLINT),
        month_int,
        month,
        CAST(COUNT(distinct workout_counter) AS SMALLINT) AS total_workouts
    FROM
        {{ ref('stg_workout_journal') }}
    GROUP BY
        year, month_int, month
    ORDER BY
        year, month_int, month
)

SELECT * FROM workouts_per_month_per_year
