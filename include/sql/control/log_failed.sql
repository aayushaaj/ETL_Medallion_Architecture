UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'FAILED',
    end_time = CURRENT_TIMESTAMP(),
    error_message= '{{error_message}}'
WHERE run_id = '{{run_id}}' 
  AND _batch_date = '{{batch_date}}' 
  AND script_name = '{{script_name}}';
