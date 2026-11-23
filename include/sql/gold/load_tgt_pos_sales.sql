-- POS sales fact (transaction grain: item, location, day, line).
-- Resolves every dimension surrogate key, so all dimensions must be
-- loaded before this task runs.
MERGE INTO RETAIL_DW.RETAIL_SCHEMA.FACT_POS_SALES AS target
USING (
    SELECT *
    FROM (
        SELECT
            HASH(p.transaction_id, p.line_id)  AS sales_line_sk,
            p.transaction_id,
            p.line_id,
            p.transaction_date,
            dd.date_sk,
            p.item_id,
            di.item_sk,
            p.location_id,
            ds.location_sk,
            p.customer_id,
            dc.customer_sk,
            p.sales_qty,
            p.unit_price,
            p.extended_amount,
            p.payment_method,
            p._batch_date,
            p._loaded_at,
            ROW_NUMBER() OVER (PARTITION BY p.transaction_id, p.line_id ORDER BY p._loaded_at DESC) AS rn
        FROM RETAIL_DW.RETAIL_SCHEMA.TMP_POS_SALES p
        LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_ITEM di
               ON p.item_id = di.item_id
              AND p.transaction_date >= di.effective_from
              AND p.transaction_date < di.effective_to
        LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_STORE ds
               ON p.location_id = ds.location_id
              AND p.transaction_date >= ds.effective_from
              AND p.transaction_date < ds.effective_to
        LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_DAY dd
               ON p.transaction_date = dd.full_date
        LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_CUSTOMER dc
               ON COALESCE(p.customer_id, 'UNKNOWN') = dc.customer_id
        WHERE p._batch_date = '{{batch_date}}'
    )
    WHERE rn = 1
) AS source
ON target.transaction_id = source.transaction_id
   AND target.line_id = source.line_id
WHEN MATCHED THEN
    UPDATE SET
        sales_line_sk    = source.sales_line_sk,
        transaction_date = source.transaction_date,
        date_sk          = source.date_sk,
        item_id          = source.item_id,
        item_sk          = source.item_sk,
        location_id      = source.location_id,
        location_sk      = source.location_sk,
        customer_id      = source.customer_id,
        customer_sk      = source.customer_sk,
        sales_qty        = source.sales_qty,
        unit_price       = source.unit_price,
        extended_amount  = source.extended_amount,
        payment_method   = source.payment_method,
        _batch_date      = source._batch_date,
        _loaded_at       = source._loaded_at
WHEN NOT MATCHED THEN
    INSERT (sales_line_sk, transaction_id, line_id, transaction_date, date_sk,
            item_id, item_sk, location_id, location_sk, customer_id, customer_sk,
            sales_qty, unit_price, extended_amount, payment_method, _batch_date, _loaded_at)
    VALUES (source.sales_line_sk, source.transaction_id, source.line_id, source.transaction_date,
            source.date_sk, source.item_id, source.item_sk, source.location_id, source.location_sk,
            source.customer_id, source.customer_sk, source.sales_qty, source.unit_price,
            source.extended_amount, source.payment_method, source._batch_date, source._loaded_at);

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_POS_SALES WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.FACT_POS_SALES WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
