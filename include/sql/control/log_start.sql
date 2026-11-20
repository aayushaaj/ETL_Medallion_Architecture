INSERT INTO RETAIL_DW.RETAIL_SCHEMA.C_PARAM (run_id, param_name, param_value)
VALUES ('{{run_id}}', 'batch_date', '{{batch_date}}');

INSERT INTO RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG (run_id, _batch_date, script_name, status, start_time)
VALUES ('{{run_id}}', '{{batch_date}}', '{{script_name}}', 'RUNNING', CURRENT_TIMESTAMP());
