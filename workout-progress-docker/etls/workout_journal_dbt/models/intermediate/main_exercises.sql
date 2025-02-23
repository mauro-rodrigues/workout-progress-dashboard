{{ config(materialized='table') }}

WITH main_exercises AS (
    SELECT
        exercise
    FROM
        {{ ref('stg_workout_journal') }}
    WHERE
        exercise IN (
            'pull-ups',
            'chin-ups',
            'push-ups',
            'dips',
            'squats',
            'australian pull-ups',
            'straight bar dips',
            'pike push-ups',
            'knee raises',
            'leg raises'
        )
)

SELECT * FROM main_exercises
