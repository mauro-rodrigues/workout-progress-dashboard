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
),
month_with_most_workouts_per_year AS (
    SELECT
        year,
        month_int,
        month,
        total_workouts,
        RANK() OVER (PARTITION BY year ORDER BY total_workouts DESC) AS rank
    FROM
        workouts_per_month_per_year
)

SELECT
    year,
    month_int,
    month,
    total_workouts
FROM
    month_with_most_workouts_per_year
WHERE
    rank = 1
ORDER BY
    year, month_int
