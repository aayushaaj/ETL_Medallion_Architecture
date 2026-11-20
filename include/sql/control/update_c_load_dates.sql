MERGE INTO RETAIL_DW.RETAIL_SCHEMA.C_LOAD_DATES AS target
USING (
    SELECT column1 AS target_table
    FROM VALUES
        ('DIM_SUBCLASS'),
        ('DIM_DISTRICT'),
        ('DIM_DAY'),
        ('DIM_CUSTOMER'),
        ('DIM_ITEM'),
        ('DIM_STORE'),
        ('FACT_POS_SALES'),
        ('FACT_INVENTORY_SNAPSHOT'),
        ('FACT_PRICE'),
        ('RPT_SALES_DAILY')
) AS source
ON target.target_table = source.target_table
WHEN MATCHED AND (target.last_load_date IS NULL
                  OR target.last_load_date <= TO_DATE('{{batch_date}}')) THEN
    UPDATE SET
        last_load_date = TO_DATE('{{batch_date}}'),
        last_run_id    = '{{run_id}}',
        updated_at     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (target_table, last_load_date, last_run_id, updated_at)
    VALUES (source.target_table, TO_DATE('{{batch_date}}'), '{{run_id}}', CURRENT_TIMESTAMP());