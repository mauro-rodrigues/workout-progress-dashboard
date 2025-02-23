{{ config(materialized='table') }}

WITH select_raw_data AS (
    SELECT
        month,
        -- convert month from string to integer (e.g., 'january' -> 1)
        CAST(
            CASE
                WHEN month = 'january' THEN 1
                WHEN month = 'february' THEN 2
                WHEN month = 'march' THEN 3
                WHEN month = 'april' THEN 4
                WHEN month = 'may' THEN 5
                WHEN month = 'june' THEN 6
                WHEN month = 'july' THEN 7
                WHEN month = 'august' THEN 8
                WHEN month = 'september' THEN 9
                WHEN month = 'october' THEN 10
                WHEN month = 'november' THEN 11
                WHEN month = 'december' THEN 12
            END AS SMALLINT
        ) AS month_int,
        weekday,
        -- convert weekday to integer (monday = 1, sunday = 7)
        CAST(
            CASE
                WHEN weekday = 'monday' THEN 1
                WHEN weekday = 'tuesday' THEN 2
                WHEN weekday = 'wednesday' THEN 3
                WHEN weekday = 'thursday' THEN 4
                WHEN weekday = 'friday' THEN 5
                WHEN weekday = 'saturday' THEN 6
                WHEN weekday = 'sunday' THEN 7
            END AS SMALLINT
        ) AS weekday_int,
        CAST(EXTRACT(YEAR FROM date) AS SMALLINT) AS year,
        date,
        CASE
            WHEN deload = 1 THEN TRUE
            WHEN deload = 0 THEN FALSE
        END AS deload_bool,
        workout_counter,
        set_counter,
        type AS workout_type,
        section AS workout_section,
        exercise,
        weight::NUMERIC(5,2) AS added_weight,
        weight_unit,
        CASE WHEN weight = 0 THEN TRUE ELSE FALSE END AS bodyweight,
        grip,
        amount,
        rest

    FROM {{ source('raw_data', 'workout_journal') }}
)

SELECT * FROM select_raw_data
