{{ config(materialized='table') }}

WITH exercises_calculated_in_reps AS (
    SELECT
        DISTINCT
            exercise
        FROM
            {{ ref('stg_workout_journal') }}
        WHERE
            exercise NOT LIKE '%(s)%' AND exercise NOT LIKE '%+%'
        ORDER BY
            exercise
)

SELECT * FROM exercises_calculated_in_reps