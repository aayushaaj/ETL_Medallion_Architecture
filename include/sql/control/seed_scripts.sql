INSERT INTO RETAIL_DW.RETAIL_SCHEMA.C_BATCH_SCRIPTS (script_name, layer, execution_order)
VALUES
    ('load_stg', 'STG', 1),
    ('load_tmp_item_master', 'TMP', 2),
    ('load_tmp_store_master', 'TMP', 3),
    ('load_tmp_pos_sales', 'TMP', 4),
    ('load_tmp_inventory_snapshot', 'TMP', 5),
    ('load_tgt_subclass', 'TGT', 6),
    ('load_tgt_district', 'TGT', 7),
    ('load_tgt_day', 'TGT', 8),
    ('load_tgt_customer', 'TGT', 9),
    ('load_tgt_item_master', 'TGT', 10),
    ('load_tgt_store_master', 'TGT', 11),
    ('load_tgt_pos_sales', 'TGT', 12),
    ('load_tgt_inventory_snapshot', 'TGT', 13),
    ('load_tgt_price', 'TGT', 14),
    ('load_tgt_sales_daily', 'TGT', 15);
