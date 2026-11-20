DELETE FROM RETAIL_DW.RETAIL_SCHEMA.STG_ITEM_MASTER WHERE _batch_date = '{{batch_date}}';

PUT file://{{data_path}}/batch_{{batch_date}}/item_master_{{batch_date}}.csv @RETAIL_DW.RETAIL_SCHEMA.LANDING_STAGE/batch_{{batch_date}}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

COPY INTO RETAIL_DW.RETAIL_SCHEMA.STG_ITEM_MASTER (
    item_id, item_name, subclass_id, subclass_name, unit_price, unit_cost, effective_from, _batch_date
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, '{{batch_date}}'
    FROM @RETAIL_DW.RETAIL_SCHEMA.LANDING_STAGE/batch_{{batch_date}}/
)
FILE_FORMAT = (FORMAT_NAME = RETAIL_DW.RETAIL_SCHEMA.CSV_FORMAT)
PATTERN = '.*item_master_{{batch_date}}\\.csv'
FORCE = TRUE
ON_ERROR = 'ABORT_STATEMENT';

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.STG_ITEM_MASTER WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.STG_ITEM_MASTER WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
