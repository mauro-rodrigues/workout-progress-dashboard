{{ config(materialized='table') }}

WITH total_seconds_per_exercise_weight AS (
    SELECT
        swj.exercise,
        CAST(SUM(swj.amount) AS SMALLINT) AS total_seconds,
        swj.added_weight,
        swj.weight_unit
    FROM
        {{ ref('stg_workout_journal') }} swj
    INNER JOIN
        {{ ref('exercises_calculated_in_seconds') }} ecis
    ON
        swj.exercise=ecis.exercise
    GROUP BY
        swj.exercise, swj.added_weight, swj.weight_unit
    ORDER BY 
        total_seconds DESC
)

SELECT * FROM total_seconds_per_exercise_weight
