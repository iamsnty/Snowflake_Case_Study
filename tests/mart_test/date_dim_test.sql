
WITH validation AS (
    SELECT
        COUNT(*) AS total_records,
        SUM(CASE WHEN DATE_PK IS NULL THEN 1 ELSE 0 END) AS missing_date_pk,
        SUM(CASE WHEN MEASUREMENT_TIME IS NULL THEN 1 ELSE 0 END) AS missing_measurement_time,
        SUM(CASE WHEN AQI_YEAR IS NULL OR AQI_YEAR < 2000 THEN 1 ELSE 0 END) AS invalid_year,
        SUM(CASE WHEN AQI_MONTH IS NULL OR AQI_MONTH NOT BETWEEN 1 AND 12 THEN 1 ELSE 0 END) AS invalid_month,
        SUM(CASE WHEN AQI_DAY IS NULL OR AQI_DAY NOT BETWEEN 1 AND 31 THEN 1 ELSE 0 END) AS invalid_day,
        SUM(CASE WHEN AQI_HOUR IS NULL OR AQI_HOUR NOT BETWEEN 0 AND 23 THEN 1 ELSE 0 END) AS invalid_hour
    FROM {{ ref('date_dim') }}  -- change this to your model name
)
SELECT *
FROM validation
WHERE
    missing_date_pk > 0
    OR missing_measurement_time > 0
    OR invalid_year > 0
    OR invalid_month > 0
    OR invalid_day > 0
    OR invalid_hour < 0
