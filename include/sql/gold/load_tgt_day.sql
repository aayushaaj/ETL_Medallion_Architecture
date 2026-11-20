MERGE INTO RETAIL_DW.RETAIL_SCHEMA.DIM_DAY AS target
USING (
    SELECT
        TO_NUMBER(TO_CHAR(cal.full_date, 'YYYYMMDD'))      AS date_sk,
        cal.full_date,
        DAYOFWEEKISO(cal.full_date)                        AS day_of_week_num,
        DAYNAME(cal.full_date)                             AS day_name,
        MONTH(cal.full_date)                               AS month_num,
        MONTHNAME(cal.full_date)                           AS month_name,
        QUARTER(cal.full_date)                             AS quarter_num,
        YEAR(cal.full_date)                                AS year_num,
        IFF(DAYOFWEEKISO(cal.full_date) > 5, TRUE, FALSE)  AS is_weekend
    FROM (
        SELECT DATEADD('day', SEQ4(), TO_DATE('2020-01-01')) AS full_date
        FROM TABLE(GENERATOR(ROWCOUNT => 3653))
    ) AS cal
) AS source
ON target.date_sk = source.date_sk
WHEN NOT MATCHED THEN
    INSERT (date_sk, full_date, day_of_week_num, day_name, month_num, month_name,
            quarter_num, year_num, is_weekend, _inserted_at, _updated_at)
    VALUES (source.date_sk, source.full_date, source.day_of_week_num, source.day_name, source.month_num,
            source.month_name, source.quarter_num, source.year_num, source.is_weekend, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = 3653,
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_DAY),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
