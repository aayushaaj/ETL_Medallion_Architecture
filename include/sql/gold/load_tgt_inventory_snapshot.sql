--inventory positional fact (one row per item, location, snapshot).
-- Truncate the batch's rows before re-loading to ensure idempotency.
DELETE FROM RETAIL_DW.RETAIL_SCHEMA.FACT_INVENTORY_SNAPSHOT
WHERE _batch_date = '{{batch_date}}';

MERGE INTO RETAIL_DW.RETAIL_SCHEMA.FACT_INVENTORY_SNAPSHOT AS target
USING (
    SELECT *
    FROM (
        SELECT
            HASH(i.item_id, i.location_id, i.snapshot_date)  AS inventory_sk,
            i.snapshot_date,
            dd.date_sk,
            i.item_id,
            di.item_sk,
            i.location_id,
            ds.location_sk,
            i.on_hand_qty,
            i.on_order_qty,
            i.unit_cost,
            i._batch_date,
            i._loaded_at,
            ROW_NUMBER() OVER (PARTITION BY i.item_id, i.location_id, i.snapshot_date ORDER BY i._loaded_at DESC) AS rn
        FROM RETAIL_DW.RETAIL_SCHEMA.TMP_INVENTORY_SNAPSHOT i
        LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_ITEM di
               ON i.item_id = di.item_id
              AND i.snapshot_date >= di.effective_from
              AND i.snapshot_date < di.effective_to
        LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_STORE ds
               ON i.location_id = ds.location_id
              AND i.snapshot_date >= ds.effective_from
              AND i.snapshot_date < ds.effective_to
        LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_DAY dd
               ON i.snapshot_date = dd.full_date
        WHERE i._batch_date = '{{batch_date}}'
    )
    WHERE rn = 1
) AS source
ON target.snapshot_date = source.snapshot_date
   AND target.item_id = source.item_id
   AND target.location_id = source.location_id
WHEN MATCHED THEN
    UPDATE SET
        inventory_sk = source.inventory_sk,
        date_sk      = source.date_sk,
        item_sk      = source.item_sk,
        location_sk  = source.location_sk,
        on_hand_qty  = source.on_hand_qty,
        on_order_qty = source.on_order_qty,
        unit_cost    = source.unit_cost,
        _batch_date  = source._batch_date,
        _loaded_at   = source._loaded_at
WHEN NOT MATCHED THEN
    INSERT (inventory_sk, snapshot_date, date_sk, item_id, item_sk, location_id,
            location_sk, on_hand_qty, on_order_qty, unit_cost, _batch_date, _loaded_at)
    VALUES (source.inventory_sk, source.snapshot_date, source.date_sk, source.item_id, source.item_sk,
            source.location_id, source.location_sk, source.on_hand_qty, source.on_order_qty,
            source.unit_cost, source._batch_date, source._loaded_at);

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_INVENTORY_SNAPSHOT WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.FACT_INVENTORY_SNAPSHOT WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
