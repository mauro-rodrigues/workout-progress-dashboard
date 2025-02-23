{{ config(materialized='table') }}

WITH daily_reps_per_exercise_weight AS (
    SELECT
        swj.date,
        swj.exercise,
        CAST(SUM(swj.amount) AS SMALLINT) AS total_reps,
        swj.added_weight,
        swj.weight_unit
    FROM
        {{ ref('stg_workout_journal') }} swj
    INNER JOIN
        {{ ref('exercises_calculated_in_reps') }} ecir
    ON
        swj.exercise=ecir.exercise
    GROUP BY
        swj.date, swj.exercise, swj.added_weight, swj.weight_unit
    ORDER BY
        swj.date
)

SELECT * FROM daily_reps_per_exercise_weight
