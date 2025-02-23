{{ config(materialized='table') }}

WITH yearly_seconds_per_exercise_weight AS (
    SELECT
        CAST(swj.year AS SMALLINT),
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
        swj.year, swj.exercise, swj.added_weight, swj.weight_unit
    ORDER BY
        swj.year, total_seconds DESC
)

SELECT * FROM yearly_seconds_per_exercise_weight
