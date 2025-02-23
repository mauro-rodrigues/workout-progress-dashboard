{{ config(materialized='table') }}

WITH exercises_calculated_in_seconds AS (
    SELECT
        DISTINCT
            exercise
        FROM
            {{ ref('stg_workout_journal') }}
        WHERE
            exercise LIKE '%(s)%'
        ORDER BY
            exercise
)

SELECT * FROM exercises_calculated_in_seconds