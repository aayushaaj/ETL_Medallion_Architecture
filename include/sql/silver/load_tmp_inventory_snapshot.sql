CREATE TABLE IF NOT EXISTS RETAIL_DW.RETAIL_SCHEMA.TMP_INVENTORY_SNAPSHOT LIKE RETAIL_DW.RETAIL_SCHEMA.STG_INVENTORY_SNAPSHOT;
DELETE FROM RETAIL_DW.RETAIL_SCHEMA.TMP_INVENTORY_SNAPSHOT WHERE _batch_date = '{{batch_date}}';
DELETE FROM RETAIL_DW.RETAIL_SCHEMA.C_REJ_TABLE WHERE _batch_date = '{{batch_date}}' AND source_table = 'STG_INVENTORY';

INSERT INTO RETAIL_DW.RETAIL_SCHEMA.C_REJ_TABLE (_batch_date, source_table, rule_rejected, payload)
WITH ranked AS (
    SELECT s.*, ROW_NUMBER() OVER(PARTITION BY s.item_id, s.location_id, s.snapshot_date ORDER BY s._loaded_at DESC) AS rn
    FROM RETAIL_DW.RETAIL_SCHEMA.STG_INVENTORY_SNAPSHOT s WHERE s._batch_date = '{{batch_date}}'
),
validated AS (
    SELECT *, 
        CASE 
            WHEN item_id IS NULL OR TRIM(item_id) = '' THEN 'NULL_FK'
            WHEN location_id IS NULL OR TRIM(location_id) = '' THEN 'NULL_FK'
            WHEN on_hand_qty < 0 THEN 'NEG_QTY'
            WHEN rn > 1 THEN 'DUP_LINE'
            ELSE NULL
        END AS rule_rejected
    FROM ranked
)
SELECT _batch_date, 'STG_INVENTORY', rule_rejected, OBJECT_CONSTRUCT(*) 
FROM validated WHERE rule_rejected IS NOT NULL;

INSERT INTO RETAIL_DW.RETAIL_SCHEMA.TMP_INVENTORY_SNAPSHOT
WITH ranked AS (
    SELECT s.*, ROW_NUMBER() OVER(PARTITION BY s.item_id, s.location_id, s.snapshot_date ORDER BY s._loaded_at DESC) AS rn
    FROM RETAIL_DW.RETAIL_SCHEMA.STG_INVENTORY_SNAPSHOT s WHERE s._batch_date = '{{batch_date}}'
),
validated AS (
    SELECT *, 
        CASE 
            WHEN item_id IS NULL OR TRIM(item_id) = '' THEN 'NULL_FK'
            WHEN location_id IS NULL OR TRIM(location_id) = '' THEN 'NULL_FK'
            WHEN on_hand_qty < 0 THEN 'NEG_QTY'
            WHEN rn > 1 THEN 'DUP_LINE'
            ELSE NULL
        END AS rule_rejected
    FROM ranked
)
SELECT * EXCLUDE (rn, rule_rejected) FROM validated WHERE rule_rejected IS NULL;

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS', end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.STG_INVENTORY_SNAPSHOT WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_INVENTORY_SNAPSHOT WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.C_REJ_TABLE WHERE _batch_date = '{{batch_date}}' AND source_table = 'STG_INVENTORY')
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
