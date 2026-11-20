-- subclass reference dimension (one row per subclass).
-- Source: Silver TMP_ITEM_MASTER (subclass_id, subclass_name).
MERGE INTO RETAIL_DW.RETAIL_SCHEMA.DIM_SUBCLASS AS target
USING (
    SELECT
        HASH(subclass_id)      AS subclass_sk,
        subclass_id,
        MAX(subclass_name)     AS subclass_name,
        MAX(_batch_date)       AS _batch_date,
        MAX(_loaded_at)        AS _loaded_at
    FROM RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER
    WHERE _batch_date = '{{batch_date}}'
      AND subclass_id IS NOT NULL
      AND TRIM(subclass_id) <> ''
    GROUP BY subclass_id
) AS source
ON target.subclass_id = source.subclass_id
WHEN MATCHED AND (target._batch_date IS NULL OR source._batch_date >= target._batch_date) THEN
    UPDATE SET
        subclass_sk   = source.subclass_sk,
        subclass_name = source.subclass_name,
        _batch_date   = source._batch_date,
        _loaded_at    = source._loaded_at
WHEN NOT MATCHED THEN
    INSERT (subclass_sk, subclass_id, subclass_name, _batch_date, _loaded_at)
    VALUES (source.subclass_sk, source.subclass_id, source.subclass_name, source._batch_date, source._loaded_at);

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(DISTINCT subclass_id) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER WHERE _batch_date = '{{batch_date}}' AND subclass_id IS NOT NULL AND TRIM(subclass_id) <> ''),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_SUBCLASS WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
