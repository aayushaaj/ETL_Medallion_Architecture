DELETE FROM RETAIL_DW.RETAIL_SCHEMA.STG_STORE_MASTER WHERE _batch_date = '{{batch_date}}';

PUT file://{{data_path}}/batch_{{batch_date}}/store_master_{{batch_date}}.csv @RETAIL_DW.RETAIL_SCHEMA.LANDING_STAGE/batch_{{batch_date}}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

COPY INTO RETAIL_DW.RETAIL_SCHEMA.STG_STORE_MASTER (
    location_id, location_name, district_id, district_name, region, effective_from, _batch_date
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, '{{batch_date}}'
    FROM @RETAIL_DW.RETAIL_SCHEMA.LANDING_STAGE/batch_{{batch_date}}/
)
FILE_FORMAT = (FORMAT_NAME = RETAIL_DW.RETAIL_SCHEMA.CSV_FORMAT)
PATTERN = '.*store_master_{{batch_date}}\\.csv'
FORCE = TRUE
ON_ERROR = 'ABORT_STATEMENT';

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.STG_STORE_MASTER WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.STG_STORE_MASTER WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
