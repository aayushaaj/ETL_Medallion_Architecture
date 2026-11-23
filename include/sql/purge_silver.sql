DELETE FROM RETAIL_DW.RETAIL_SCHEMA.TMP_item_master
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.TMP_store_master
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.TMP_pos_sales
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.TMP_inventory_snapshot
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.C_REJ_TABLE
WHERE _batch_date < '{{cutoff}}';