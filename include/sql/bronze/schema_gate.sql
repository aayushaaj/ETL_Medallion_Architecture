SELECT MIN(row_count) FROM (
    SELECT COUNT(*) AS row_count FROM RETAIL_DW.RETAIL_SCHEMA.STG_POS_SALES          WHERE _batch_date = '{{batch_date}}'
    UNION ALL
    SELECT COUNT(*) AS row_count FROM RETAIL_DW.RETAIL_SCHEMA.STG_INVENTORY_SNAPSHOT WHERE _batch_date = '{{batch_date}}'
    UNION ALL
    SELECT COUNT(*) AS row_count FROM RETAIL_DW.RETAIL_SCHEMA.STG_ITEM_MASTER        WHERE _batch_date = '{{batch_date}}'
    UNION ALL
    SELECT COUNT(*) AS row_count FROM RETAIL_DW.RETAIL_SCHEMA.STG_STORE_MASTER       WHERE _batch_date = '{{batch_date}}'
);
