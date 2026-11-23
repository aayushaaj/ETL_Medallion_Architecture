-- Reference dimensions
DELETE FROM RETAIL_DW.RETAIL_SCHEMA.subclass
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.district
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.day
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.customer
WHERE _batch_date < '{{cutoff}}';

-- Core dimensions
DELETE FROM RETAIL_DW.RETAIL_SCHEMA.item_master
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.store_master
WHERE _batch_date < '{{cutoff}}';

-- Facts
DELETE FROM RETAIL_DW.RETAIL_SCHEMA.pos_sales
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.inventory_snapshot
WHERE _batch_date < '{{cutoff}}';

DELETE FROM RETAIL_DW.RETAIL_SCHEMA.price
WHERE _batch_date < '{{cutoff}}';

-- Marts
DELETE FROM RETAIL_DW.RETAIL_SCHEMA.sales_daily
WHERE _batch_date < '{{cutoff}}';