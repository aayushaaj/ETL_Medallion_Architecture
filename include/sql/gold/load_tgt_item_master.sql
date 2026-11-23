-- item dimension (SCD Type 2: historical tracking for price, cost, subclass changes).
-- Looks up subclass_sk from DIM_SUBCLASS, which must be loaded first.

-- Step 1: Expire previous active version when tracked attributes change
UPDATE RETAIL_DW.RETAIL_SCHEMA.DIM_ITEM AS target
SET effective_to = source.effective_from,
    is_current   = FALSE,
    _loaded_at   = CURRENT_TIMESTAMP()
FROM (
    SELECT i.item_id, i.effective_from, i.item_name, i.subclass_id, i.unit_price, i.unit_cost
    FROM RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER i
    WHERE i._batch_date = '{{batch_date}}'
) AS source
WHERE target.item_id = source.item_id
  AND target.is_current = TRUE
  AND target.effective_from < source.effective_from
  AND (
      COALESCE(target.subclass_id, '') <> COALESCE(source.subclass_id, '')
      OR COALESCE(target.item_name, '') <> COALESCE(source.item_name, '')
      OR COALESCE(target.unit_price, -1) <> COALESCE(source.unit_price, -1)
      OR COALESCE(target.unit_cost, -1) <> COALESCE(source.unit_cost, -1)
  );

-- Step 2: Merge incoming version (keyed on surrogate key item_id + effective_from)
MERGE INTO RETAIL_DW.RETAIL_SCHEMA.DIM_ITEM AS target
USING (
    SELECT
        HASH(i.item_id, i.effective_from) AS item_sk,
        i.item_id,
        i.item_name,
        i.subclass_id,
        sc.subclass_sk,
        i.subclass_name,
        i.unit_price,
        i.unit_cost,
        i.effective_from,
        TO_DATE('9999-12-31')             AS effective_to,
        TRUE                              AS is_current,
        i._batch_date,
        i._loaded_at
    FROM RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER i
    LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_SUBCLASS sc
           ON i.subclass_id = sc.subclass_id
    WHERE i._batch_date = '{{batch_date}}'
) AS source
ON target.item_sk = source.item_sk
WHEN MATCHED THEN
    UPDATE SET
        item_name     = source.item_name,
        subclass_id   = source.subclass_id,
        subclass_sk   = source.subclass_sk,
        subclass_name = source.subclass_name,
        unit_price    = source.unit_price,
        unit_cost     = source.unit_cost,
        _batch_date   = source._batch_date,
        _loaded_at    = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (item_sk, item_id, item_name, subclass_id, subclass_sk, subclass_name,
            unit_price, unit_cost, effective_from, effective_to, is_current, _batch_date, _loaded_at)
    VALUES (source.item_sk, source.item_id, source.item_name, source.subclass_id, source.subclass_sk,
            source.subclass_name, source.unit_price, source.unit_cost, source.effective_from,
            source.effective_to, source.is_current, source._batch_date, source._loaded_at);

-- Step 3: Audit log
UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_ITEM WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
