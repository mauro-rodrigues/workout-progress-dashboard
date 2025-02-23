{{ config(materialized='table') }}

WITH added_weight_reps AS (
    SELECT
        CAST(year AS SMALLINT),
        month_int,
        month,
        exercise,
        added_weight,
        amount,
        weight_unit
    FROM
        {{ ref('stg_workout_journal') }}
    WHERE
        NOT bodyweight
),
total_added_volume_lifted AS (
    SELECT
        awr.year,
        awr.month_int,
        awr.month,
        awr.exercise,
        SUM(awr.added_weight * amount) AS total_added_volume,
        awr.weight_unit
    FROM
        added_weight_reps awr
    INNER JOIN
        {{ ref('exercises_calculated_in_reps') }} ecir
    ON
        awr.exercise=ecir.exercise
    GROUP BY
        awr.year, awr.month_int, awr.month, awr.exercise, awr.weight_unit
    ORDER BY
        awr.year, awr.month_int, awr.exercise
)

SELECT * FROM total_added_volume_lifted
