-- =========================
-- dim_date
-- =========================

WITH RECURSIVE calendar(d) AS (
    SELECT DATE('2020-01-01')
    UNION ALL
    SELECT DATE(d, '+1 day')
    FROM calendar
    WHERE d < DATE('2035-12-31')
)
, dim_date (
    SELECT
        CAST(strftime('%Y%m%d', d) AS INTEGER)      AS date_key,
        d                                           AS full_date,
        CAST(strftime('%Y', d) AS INTEGER)          AS year,
        CAST(strftime('%m', d) AS INTEGER)          AS month,
        CAST(strftime('%d', d) AS INTEGER)          AS day_of_month,
        ((CAST(strftime('%m', d) AS INTEGER) - 1) / 3) + 1 AS quarter,
        ((strftime('%w', d) + 6) % 7) + 1            AS day_of_week
    FROM calendar;
)

select * from dim_date;