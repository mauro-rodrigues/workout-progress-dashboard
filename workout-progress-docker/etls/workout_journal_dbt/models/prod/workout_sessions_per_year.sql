{{ config(materialized='table') }}

WITH workout_sessions_per_year AS (
    SELECT
        CAST(year AS SMALLINT),
        CAST(COUNT(distinct workout_counter) AS SMALLINT) AS total_workouts
    FROM
        {{ ref('stg_workout_journal') }}
    GROUP BY
        year
    ORDER BY
        year
)

SELECT * FROM workout_sessions_per_year
