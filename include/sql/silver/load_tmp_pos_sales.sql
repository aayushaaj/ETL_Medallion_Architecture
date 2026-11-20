CREATE TABLE IF NOT EXISTS RETAIL_DW.RETAIL_SCHEMA.TMP_POS_SALES LIKE RETAIL_DW.RETAIL_SCHEMA.STG_POS_SALES;
DELETE FROM RETAIL_DW.RETAIL_SCHEMA.TMP_POS_SALES WHERE _batch_date = '{{batch_date}}';

INSERT INTO RETAIL_DW.RETAIL_SCHEMA.C_REJ_TABLE (_batch_date, source_table, rule_rejected, payload)
WITH base AS (
    SELECT s.*, im.item_id AS im_item_id,
        ROW_NUMBER() OVER(PARTITION BY s.transaction_id, s.line_id ORDER BY s._loaded_at DESC) AS rn
    FROM RETAIL_DW.RETAIL_SCHEMA.STG_POS_SALES s
    LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER im ON s.item_id = im.item_id AND im._batch_date = s._batch_date
    WHERE s._batch_date = '{{batch_date}}'
),
validated AS (
    SELECT *, 
        CASE 
            WHEN item_id IS NULL OR TRIM(item_id) = '' THEN 'NULL_FK'
            WHEN location_id IS NULL OR TRIM(location_id) = '' THEN 'NULL_FK'
            WHEN sales_qty < 0 THEN 'NEG_QTY'
            WHEN rn > 1 THEN 'DUP_LINE'
            WHEN im_item_id IS NULL THEN 'ORPHAN_FK'
            ELSE NULL
        END AS rule_rejected
    FROM base
)
SELECT _batch_date, 'STG_POS_SALES', rule_rejected, OBJECT_CONSTRUCT(*) 
FROM validated WHERE rule_rejected IS NOT NULL;

INSERT INTO RETAIL_DW.RETAIL_SCHEMA.TMP_POS_SALES
WITH base AS (
    SELECT s.*, im.item_id AS im_item_id,
        ROW_NUMBER() OVER(PARTITION BY s.transaction_id, s.line_id ORDER BY s._loaded_at DESC) AS rn
    FROM RETAIL_DW.RETAIL_SCHEMA.STG_POS_SALES s
    LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER im ON s.item_id = im.item_id AND im._batch_date = s._batch_date
    WHERE s._batch_date = '{{batch_date}}'
),
validated AS (
    SELECT *, 
        CASE 
            WHEN item_id IS NULL OR TRIM(item_id) = '' THEN 'NULL_FK'
            WHEN location_id IS NULL OR TRIM(location_id) = '' THEN 'NULL_FK'
            WHEN sales_qty < 0 THEN 'NEG_QTY'
            WHEN rn > 1 THEN 'DUP_LINE'
            WHEN im_item_id IS NULL THEN 'ORPHAN_FK'
            ELSE NULL
        END AS rule_rejected
    FROM base
)
SELECT * EXCLUDE (im_item_id, rn, rule_rejected) FROM validated WHERE rule_rejected IS NULL;

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.STG_POS_SALES WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_POS_SALES WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.C_REJ_TABLE WHERE _batch_date = '{{batch_date}}' AND source_table = 'STG_POS_SALES')
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
