-- =========================
-- dim_date
-- =========================

INSERT INTO dim_date (
    date_key,
    full_date,
    year,
    month,
    day_of_month,
    quarter,
    day_of_week
)
WITH RECURSIVE calendar AS (
    SELECT DATE '2020-01-01' AS d
    UNION ALL
    SELECT d + INTERVAL '1 day'
    FROM calendar
    WHERE d < DATE '2035-12-31'
)
SELECT
    CAST(strftime('%Y%m%d', d) AS INTEGER)      AS date_key,
    d                                           AS full_date,
    YEAR(d)                                     AS year,
    MONTH(d)                                    AS month,
    DAY(d)                                      AS day_of_month,
    QUARTER(d)                                  AS quarter,
    (EXTRACT(DOW FROM d) + 1)                   AS day_of_week
FROM calendar;