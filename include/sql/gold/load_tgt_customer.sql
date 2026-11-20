MERGE INTO RETAIL_DW.RETAIL_SCHEMA.DIM_CUSTOMER AS target
USING (
    SELECT
        HASH(customer_id)        AS customer_sk,
        customer_id,
        MAX(customer_type)       AS customer_type,
        MIN(first_seen_date)     AS first_seen_date,
        MAX(last_seen_date)      AS last_seen_date
    FROM (
        SELECT
            'UNKNOWN'              AS customer_id,
            'UNKNOWN'              AS customer_type,
            CAST(NULL AS DATE)     AS first_seen_date,
            CAST(NULL AS DATE)     AS last_seen_date
        UNION ALL
        SELECT DISTINCT
            customer_id,
            'POS'                  AS customer_type,
            _batch_date            AS first_seen_date,
            _batch_date            AS last_seen_date
        FROM RETAIL_DW.RETAIL_SCHEMA.TMP_POS_SALES
        WHERE _batch_date = '{{batch_date}}'
          AND customer_id IS NOT NULL
          AND TRIM(customer_id) <> ''
    )
    GROUP BY customer_id
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
    UPDATE SET
        customer_sk     = source.customer_sk,
        first_seen_date = COALESCE(target.first_seen_date, source.first_seen_date),
        last_seen_date  = GREATEST(COALESCE(target.last_seen_date, source.last_seen_date),
                                   COALESCE(source.last_seen_date, target.last_seen_date)),
        _batch_date     = '{{batch_date}}',
        _loaded_at      = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (customer_sk, customer_id, customer_type, first_seen_date, last_seen_date, _batch_date, _loaded_at)
    VALUES (source.customer_sk, source.customer_id, source.customer_type, source.first_seen_date,
            source.last_seen_date, '{{batch_date}}', CURRENT_TIMESTAMP());

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(DISTINCT customer_id) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_POS_SALES WHERE _batch_date = '{{batch_date}}' AND customer_id IS NOT NULL AND TRIM(customer_id) <> ''),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_CUSTOMER WHERE _batch_date = '{{batch_date}}' AND customer_id <> 'UNKNOWN'),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
