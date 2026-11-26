WITH Digits AS (
    SELECT 0 AS Digit UNION ALL SELECT 1 UNION ALL SELECT 2
    UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
    UNION ALL SELECT 9
),
DayOffsets AS (
    SELECT
        ones.Digit + 10 * tens.Digit + 100 * hundreds.Digit
            + 1000 * thousands.Digit AS DayOffset
    FROM Digits AS ones
    CROSS JOIN Digits AS tens
    CROSS JOIN Digits AS hundreds
    CROSS JOIN Digits AS thousands
),
DateSpine AS (
    SELECT
        DATEADD(day, DayOffset, CAST('2015-01-01' AS DATE)) AS CalendarDate
    FROM DayOffsets
    WHERE DayOffset <= DATEDIFF(day, CAST('2015-01-01' AS DATE), CAST('2035-12-31' AS DATE))
)

SELECT
    -- Surrogate Key: Smart Date Key format (YYYYMMDD) as an integer
    CAST(FORMAT(CalendarDate, 'yyyyMMdd') AS INT) AS s_dim_date_sk,

    CalendarDate AS full_dt,
    DATEPART(year, CalendarDate) AS calendar_year_num,
    DATEPART(quarter, CalendarDate) AS calendar_quarter_num,
    CONCAT('Q', DATEPART(quarter, CalendarDate)) AS calendar_quarter_nm,
    DATEPART(month, CalendarDate) AS month_of_year_num,
    CAST(FORMAT(CalendarDate, 'MMMM') AS VARCHAR(50)) AS month_nm,
    CAST(FORMAT(CalendarDate, 'MMM') AS VARCHAR(50)) AS month_short_nm,
    DATEPART(week, CalendarDate) AS week_of_year_num,
    DATEPART(day, CalendarDate) AS day_of_month_num,
    DATEPART(dayofyear, CalendarDate) AS day_of_year_num,
    DATEPART(weekday, CalendarDate) AS day_of_week_num,
    CAST(FORMAT(CalendarDate, 'dddd') AS VARCHAR(50)) AS day_nm,
    CAST(FORMAT(CalendarDate, 'ddd') AS VARCHAR(50)) AS day_short_nm,

    -- Business Flags
    CASE
        WHEN DATEPART(weekday, CalendarDate) IN (1, 7) THEN 1
        ELSE 0
    END AS is_weekend_flag,

    -- Year-Month string identifier (YYYY-MM)
    CAST(FORMAT(CalendarDate, 'yyyy-MM') AS VARCHAR(7)) AS year_month_cd

FROM DateSpine
