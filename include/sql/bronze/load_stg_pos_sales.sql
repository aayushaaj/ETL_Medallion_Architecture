DELETE FROM RETAIL_DW.RETAIL_SCHEMA.STG_POS_SALES WHERE _batch_date = '{{batch_date}}';

PUT file://{{data_path}}/batch_{{batch_date}}/pos_sales_{{batch_date}}.csv @RETAIL_DW.RETAIL_SCHEMA.LANDING_STAGE/batch_{{batch_date}}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

COPY INTO RETAIL_DW.RETAIL_SCHEMA.STG_POS_SALES (
    transaction_id, line_id, transaction_date, item_id, location_id,
    customer_id, sales_qty, unit_price, extended_amount, payment_method, _batch_date
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, '{{batch_date}}'
    FROM @RETAIL_DW.RETAIL_SCHEMA.LANDING_STAGE/batch_{{batch_date}}/
)
FILE_FORMAT = (FORMAT_NAME = RETAIL_DW.RETAIL_SCHEMA.CSV_FORMAT)
PATTERN = '.*pos_sales_{{batch_date}}\\.csv'
FORCE = TRUE
ON_ERROR = 'ABORT_STATEMENT';

UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM  RETAIL_DW.RETAIL_SCHEMA.STG_POS_SALES WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM  RETAIL_DW.RETAIL_SCHEMA.STG_POS_SALES WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
