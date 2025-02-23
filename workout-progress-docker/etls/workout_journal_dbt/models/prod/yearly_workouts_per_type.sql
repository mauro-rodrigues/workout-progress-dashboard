{{ config(materialized='table') }}

WITH yearly_workouts_per_type AS (
    SELECT
        CAST(year AS SMALLINT),
        workout_type,
        CAST(COUNT(distinct workout_counter) AS SMALLINT) AS total_workouts
    FROM
        {{ ref('stg_workout_journal') }}
    GROUP BY
        year, workout_type
    ORDER BY
        year, total_workouts DESC
)

SELECT * FROM yearly_workouts_per_type
