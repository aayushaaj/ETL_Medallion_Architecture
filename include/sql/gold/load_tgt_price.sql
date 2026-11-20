--  price positional fact (item price per effective date).
-- Only item_master carries price, and it has no location column, so the
-- grain is item x effective_from. The diagram's "per item / location /
-- day" grain cannot be built from the available data.
MERGE INTO RETAIL_DW.RETAIL_SCHEMA.FACT_PRICE AS target
USING (
    SELECT
        HASH(di.item_id, di.effective_from)               AS price_sk,
        di.effective_from,
        dd.date_sk,
        di.item_id,
        di.item_sk,
        di.unit_price,
        di.unit_cost,
        ROUND(di.unit_price - di.unit_cost, 2)            AS unit_margin,
        di._batch_date,
        di._loaded_at
    FROM RETAIL_DW.RETAIL_SCHEMA.DIM_ITEM di
    LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_DAY dd
           ON di.effective_from = dd.full_date
    WHERE di._batch_date = '{{batch_date}}'
) AS source
ON target.item_id = source.item_id
   AND target.effective_from = source.effective_from
WHEN MATCHED THEN
    UPDATE SET
        price_sk  = source.price_sk,
        date_sk   = source.date_sk,
        item_sk   = source.item_sk,
        unit_price = source.unit_price,
        unit_cost  = source.unit_cost,
        unit_margin = source.unit_margin,
        _batch_date = source._batch_date,
        _loaded_at  = source._loaded_at,
        _updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (price_sk, effective_from, date_sk, item_id, item_sk,
            unit_price, unit_cost, unit_margin, _batch_date, _loaded_at, _inserted_at, _updated_at)
    VALUES (source.price_sk, source.effective_from, source.date_sk, source.item_id, source.item_sk,
            source.unit_price, source.unit_cost, source.unit_margin, source._batch_date, source._loaded_at, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_ITEM WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.FACT_PRICE WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
