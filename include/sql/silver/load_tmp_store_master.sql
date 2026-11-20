CREATE TABLE IF NOT EXISTS RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER LIKE RETAIL_DW.RETAIL_SCHEMA.STG_STORE_MASTER;
DELETE FROM RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER WHERE _batch_date = '{{batch_date}}';

INSERT INTO RETAIL_DW.RETAIL_SCHEMA.C_REJ_TABLE (_batch_date, source_table, rule_rejected, payload)
WITH ranked AS (
    SELECT s.*, ROW_NUMBER() OVER(PARTITION BY s.location_id ORDER BY s._loaded_at DESC) AS rn
    FROM RETAIL_DW.RETAIL_SCHEMA.STG_STORE_MASTER s WHERE s._batch_date = '{{batch_date}}'
),
validated AS (
    SELECT *, 
        CASE 
            WHEN location_id IS NULL OR TRIM(location_id) = '' THEN 'NULL_PK'
            WHEN rn > 1 THEN 'DUP_PK'
            WHEN effective_from IS NULL THEN 'INVALID_EFFECTIVE_FROM'
            ELSE NULL
        END AS rule_rejected
    FROM ranked
)
SELECT _batch_date, 'STG_STORE_MASTER', rule_rejected, OBJECT_CONSTRUCT(*) 
FROM validated WHERE rule_rejected IS NOT NULL;

INSERT INTO RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER
WITH ranked AS (
    SELECT s.*, ROW_NUMBER() OVER(PARTITION BY s.location_id ORDER BY s._loaded_at DESC) AS rn
    FROM RETAIL_DW.RETAIL_SCHEMA.STG_STORE_MASTER s WHERE s._batch_date = '{{batch_date}}'
),
validated AS (
    SELECT *, 
        CASE 
            WHEN location_id IS NULL OR TRIM(location_id) = '' THEN 'NULL_PK'
            WHEN rn > 1 THEN 'DUP_PK'
            WHEN effective_from IS NULL THEN 'INVALID_EFFECTIVE_FROM'
            ELSE NULL
        END AS rule_rejected
    FROM ranked
)
SELECT * EXCLUDE (rn, rule_rejected) FROM validated WHERE rule_rejected IS NULL;

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS', end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.STG_STORE_MASTER WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.C_REJ_TABLE WHERE _batch_date = '{{batch_date}}' AND source_table = 'STG_STORE_MASTER')
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
