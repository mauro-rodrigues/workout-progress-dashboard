{{ config(materialized='table') }}

WITH yearly_reps_per_exercise_weight AS (
    SELECT
        CAST(swj.year AS SMALLINT),
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
        swj.year, swj.exercise, swj.added_weight, swj.weight_unit
    ORDER BY
        swj.year, total_reps DESC
)

SELECT * FROM yearly_reps_per_exercise_weight
