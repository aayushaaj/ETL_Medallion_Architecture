DELETE FROM RETAIL_DW.RETAIL_SCHEMA.STG_item_master
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.STG_store_master
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.STG_pos_sales
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.STG_inventory_snapshot
WHERE _batch_date < '{{cutoff}}';