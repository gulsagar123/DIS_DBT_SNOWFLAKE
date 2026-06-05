{{
    config(
        materialized='table',
        schema='SILVER_DBT',
        alias='DIM_DATE'
    )
}}

/*
    Silver Layer - DIM_DATE
    ------------------------
    - No staging layer needed
    - Full refresh table - truncate and reload
    - Date range: 2020-01-01 to 2030-12-31
    - Fiscal Year: Oct - Sep
*/

WITH date_series AS (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))  -- days between 2020-01-01 and 2030-12-31
),

fiscal_calc AS (
    SELECT
        calendar_date,

        -- Fiscal Year (Oct-Sep)
        CASE
            WHEN MONTH(calendar_date) >= 10 THEN YEAR(calendar_date) + 1
            ELSE YEAR(calendar_date)
        END AS fiscal_year,

        CASE
            WHEN MONTH(calendar_date) >= 10 THEN DATE_FROM_PARTS(YEAR(calendar_date),     10, 1)
            ELSE                                  DATE_FROM_PARTS(YEAR(calendar_date) - 1, 10, 1)
        END AS first_day_of_fiscal_year,

        CASE
            WHEN MONTH(calendar_date) >= 10 THEN DATE_FROM_PARTS(YEAR(calendar_date) + 1, 9, 30)
            ELSE                                  DATE_FROM_PARTS(YEAR(calendar_date),     9, 30)
        END AS last_day_of_fiscal_year,

        -- Fiscal Quarter
        CASE
            WHEN MONTH(calendar_date) IN (10,11,12) THEN 1
            WHEN MONTH(calendar_date) IN (1,2,3)    THEN 2
            WHEN MONTH(calendar_date) IN (4,5,6)    THEN 3
            ELSE 4
        END AS fiscal_quarter,

        -- Fiscal Month (1=Oct ... 12=Sep)
        CASE
            WHEN MONTH(calendar_date) >= 10 THEN MONTH(calendar_date) - 9
            ELSE                                  MONTH(calendar_date) + 3
        END AS fiscal_month,

        -- ISO Week Start (Monday) and End (Sunday)
        DATEADD(DAY, -(DAYOFWEEKISO(calendar_date) - 1), calendar_date) AS week_start,
        DATEADD(DAY,  (7 - DAYOFWEEKISO(calendar_date)), calendar_date) AS week_end

    FROM date_series
),

week_assigned AS (
    SELECT
        fc.*,
        DATEADD(DAY, -(DAYOFWEEKISO(fc.calendar_date) - 4), fc.calendar_date) AS thursday_of_week
    FROM fiscal_calc fc
)

SELECT
    -- Keys
    TO_CHAR(calendar_date, 'YYYYMMDD')                                      AS DATE_KEY,
    calendar_date                                                            AS CALENDAR_DATE,

    -- Calendar Year
    YEAR(calendar_date)                                                      AS YEAR,
    DATE_TRUNC('YEAR', calendar_date)                                        AS FIRST_DAY_OF_YEAR,
    LAST_DAY(calendar_date, 'YEAR')                                          AS LAST_DAY_OF_YEAR,
    DATE_TRUNC('YEAR', DATEADD(YEAR, -1, calendar_date))                     AS PREVIOUS_YEAR_FIRST_DAY,
    LAST_DAY(DATEADD(YEAR, -1, calendar_date), 'YEAR')                       AS PREVIOUS_YEAR_LAST_DAY,
    YEAROFWEEKISO(calendar_date)                                             AS ISO_YEAR,

    -- Fiscal Year
    wa.fiscal_year                                                           AS FISCAL_YEAR,
    'FY' || wa.fiscal_year                                                   AS FISCAL_YEAR_LABEL,
    wa.first_day_of_fiscal_year                                              AS FIRST_DAY_OF_FISCAL_YEAR,
    wa.last_day_of_fiscal_year                                               AS LAST_DAY_OF_FISCAL_YEAR,

    -- Fiscal Quarter
    wa.fiscal_quarter                                                        AS FISCAL_QUARTER,
    'FQ' || wa.fiscal_quarter                                                AS FISCAL_QUARTER_NAME,
    'FQ' || wa.fiscal_quarter || '-FY' || wa.fiscal_year                     AS FISCAL_QUARTER_YEAR,

    -- Fiscal Month
    wa.fiscal_month                                                          AS FISCAL_MONTH,

    -- Fiscal Week
    DATEDIFF(
        WEEK,
        wa.first_day_of_fiscal_year,
        DATE_TRUNC('WEEK', calendar_date)
    ) + 1                                                                    AS FISCAL_WEEK_NUMBER,
    'FW' || LPAD(DATEDIFF(WEEK, wa.first_day_of_fiscal_year, DATE_TRUNC('WEEK', calendar_date)) + 1, 2, '0')
         || '-FY' || wa.fiscal_year                                          AS FISCAL_WEEK_LABEL,

    -- Calendar Quarter
    QUARTER(calendar_date)                                                   AS QUARTER,
    'Q' || QUARTER(calendar_date)                                            AS QUARTER_NAME,
    'Q' || QUARTER(calendar_date) || '-' || YEAR(calendar_date)              AS QUARTER_YEAR,
    DATE_TRUNC('QUARTER', calendar_date)                                     AS FIRST_DAY_OF_QUARTER,
    LAST_DAY(calendar_date, 'QUARTER')                                       AS LAST_DAY_OF_QUARTER,
    DATE_TRUNC('QUARTER', DATEADD(QUARTER, -1, calendar_date))               AS PREVIOUS_QUARTER_FIRST_DAY,
    LAST_DAY(DATEADD(QUARTER, -1, calendar_date), 'QUARTER')                 AS PREVIOUS_QUARTER_LAST_DAY,

    -- Calendar Month
    MONTH(calendar_date)                                                     AS MONTH,
    TO_CHAR(calendar_date, 'MMMM')                                           AS MONTH_NAME,
    TO_CHAR(calendar_date, 'YYYY-MM')                                        AS YEAR_MONTH_DESC,
    TO_CHAR(calendar_date, 'YYYY-MM')                                        AS YEAR_MONTH,
    DATE_TRUNC('MONTH', calendar_date)                                       AS FIRST_DAY_OF_MONTH,
    LAST_DAY(calendar_date)                                                  AS LAST_DAY_OF_MONTH,
    DATE_TRUNC('MONTH', DATEADD(MONTH, -1, calendar_date))                   AS PREVIOUS_MONTH_FIRST_DAY,
    LAST_DAY(DATEADD(MONTH, -1, calendar_date))                              AS PREVIOUS_MONTH_LAST_DAY,
    DAY(LAST_DAY(calendar_date))                                             AS DAYS_IN_MONTH,

    -- Week
    WEEKOFYEAR(calendar_date)                                                AS WEEK_OF_YEAR,
    wa.week_start                                                            AS WEEK_START,
    wa.week_end                                                              AS WEEK_END,

    -- Week Assigned
    CASE
        WHEN MONTH(wa.thursday_of_week) >= 10 THEN YEAR(wa.thursday_of_week) + 1
        ELSE YEAR(wa.thursday_of_week)
    END                                                                      AS WEEK_ASSIGNED_FISCAL_YEAR,
    CASE
        WHEN MONTH(wa.thursday_of_week) >= 10 THEN YEAR(wa.thursday_of_week) + 1
        ELSE YEAR(wa.thursday_of_week)
    END * 100 +
    CASE
        WHEN MONTH(wa.thursday_of_week) IN (10,11,12) THEN 1
        WHEN MONTH(wa.thursday_of_week) IN (1,2,3)    THEN 2
        WHEN MONTH(wa.thursday_of_week) IN (4,5,6)    THEN 3
        ELSE 4
    END                                                                      AS WEEK_ASSIGNED_FISCAL_YEAR_SORT,
    CASE
        WHEN MONTH(wa.thursday_of_week) >= 10 THEN MONTH(wa.thursday_of_week) - 9
        ELSE MONTH(wa.thursday_of_week) + 3
    END                                                                      AS WEEK_ASSIGNED_FISCAL_MONTH,
    CASE
        WHEN MONTH(wa.thursday_of_week) IN (10,11,12) THEN 1
        WHEN MONTH(wa.thursday_of_week) IN (1,2,3)    THEN 2
        WHEN MONTH(wa.thursday_of_week) IN (4,5,6)    THEN 3
        ELSE 4
    END                                                                      AS WEEK_ASSIGNED_FISCAL_QUARTER,
    YEAR(wa.thursday_of_week)                                                AS WEEK_ASSIGNED_YEAR,
    MONTH(wa.thursday_of_week)                                               AS WEEK_ASSIGNED_MONTH,
    QUARTER(wa.thursday_of_week)                                             AS WEEK_ASSIGNED_QUARTER,
    YEAR(wa.thursday_of_week) * 10 + QUARTER(wa.thursday_of_week)           AS WEEK_ASSIGNED_QUARTER_SORT,
    TO_CHAR(wa.thursday_of_week, 'YYYY-MM')                                  AS WEEK_ASSIGNED_YEAR_MONTH,

    -- Day
    DAYOFYEAR(calendar_date)                                                 AS DAY_OF_YEAR,
    DAY(calendar_date)                                                       AS DAY_OF_MONTH,
    DAYOFWEEK(calendar_date)                                                 AS DAY_OF_WEEK,
    DAYNAME(calendar_date)                                                   AS DAY_NAME,
    WEEKISO(calendar_date)                                                   AS ISO_WEEK,
    DAYOFWEEKISO(calendar_date)                                              AS ISO_DAY,
    CASE WHEN DAYOFWEEK(calendar_date) IN (0,6) THEN TRUE ELSE FALSE END     AS WEEKEND_FLAG,
    FALSE                                                                    AS ACC_HOLIDAY_OBSERVED_FLAG,

    -- Current Flags
    CASE WHEN YEAR(calendar_date) = YEAR(CURRENT_DATE) THEN TRUE ELSE FALSE END
                                                                             AS IS_CURRENT_YEAR,
    CASE WHEN DATE_TRUNC('QUARTER', calendar_date) = DATE_TRUNC('QUARTER', CURRENT_DATE) THEN TRUE ELSE FALSE END
                                                                             AS IS_CURRENT_QUARTER,
    CASE WHEN DATE_TRUNC('MONTH', calendar_date) = DATE_TRUNC('MONTH', CURRENT_DATE) THEN TRUE ELSE FALSE END
                                                                             AS IS_CURRENT_MONTH,
    CASE WHEN calendar_date = CURRENT_DATE THEN TRUE ELSE FALSE END          AS IS_TODAY,
    CASE WHEN wa.fiscal_year = (CASE WHEN MONTH(CURRENT_DATE) >= 10 THEN YEAR(CURRENT_DATE)+1 ELSE YEAR(CURRENT_DATE) END)
         THEN TRUE ELSE FALSE END                                            AS IS_CURRENT_FISCAL_YEAR,
    CASE WHEN wa.fiscal_year = (CASE WHEN MONTH(CURRENT_DATE) >= 10 THEN YEAR(CURRENT_DATE)+1 ELSE YEAR(CURRENT_DATE) END)
          AND wa.fiscal_quarter = (CASE WHEN MONTH(CURRENT_DATE) IN (10,11,12) THEN 1
                                        WHEN MONTH(CURRENT_DATE) IN (1,2,3)   THEN 2
                                        WHEN MONTH(CURRENT_DATE) IN (4,5,6)   THEN 3 ELSE 4 END)
         THEN TRUE ELSE FALSE END                                            AS IS_CURRENT_FISCAL_QUARTER,
    CASE WHEN wa.fiscal_year = (CASE WHEN MONTH(CURRENT_DATE) >= 10 THEN YEAR(CURRENT_DATE)+1 ELSE YEAR(CURRENT_DATE) END)
          AND wa.fiscal_month = (CASE WHEN MONTH(CURRENT_DATE) >= 10 THEN MONTH(CURRENT_DATE)-9 ELSE MONTH(CURRENT_DATE)+3 END)
         THEN TRUE ELSE FALSE END                                            AS IS_CURRENT_FISCAL_MONTH,

    -- Audit
    CURRENT_TIMESTAMP()                                                      AS ETL_INSERTED_DATE,
    'ETL_PROCESS'                                                            AS ETL_INSERTED_BY,
    CURRENT_TIMESTAMP()                                                      AS ETL_UPDATED_DATE,
    'ETL_PROCESS'                                                            AS ETL_UPDATED_BY

FROM week_assigned wa