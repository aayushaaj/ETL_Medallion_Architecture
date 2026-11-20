-- One row per dataset: the Silver row count must equal the Gold row count.
-- The DAG task fails the run when any check returns FAIL.
WITH checks AS (
    SELECT
        'ITEM_MASTER'   AS check_name,
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER
          WHERE _batch_date = '{{batch_date}}')                                   AS source_rows,
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_ITEM
          WHERE _batch_date = '{{batch_date}}')                                   AS target_rows

    UNION ALL
    SELECT
        'STORE_MASTER',
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER
          WHERE _batch_date = '{{batch_date}}'),
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_STORE
          WHERE _batch_date = '{{batch_date}}')

    UNION ALL
    SELECT
        'SUBCLASS',
        (SELECT COUNT(DISTINCT subclass_id) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER
          WHERE _batch_date = '{{batch_date}}' AND subclass_id IS NOT NULL),
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_SUBCLASS
          WHERE _batch_date = '{{batch_date}}')

    UNION ALL
    SELECT
        'DISTRICT',
        (SELECT COUNT(DISTINCT district_id) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER
          WHERE _batch_date = '{{batch_date}}' AND district_id IS NOT NULL),
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_DISTRICT
          WHERE _batch_date = '{{batch_date}}')

    UNION ALL
    SELECT
        'CUSTOMER',
        (SELECT COUNT(DISTINCT customer_id) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_POS_SALES
          WHERE _batch_date = '{{batch_date}}'
            AND customer_id IS NOT NULL AND TRIM(customer_id) <> ''),
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_CUSTOMER
          WHERE _batch_date = '{{batch_date}}' AND customer_id <> 'UNKNOWN')

    UNION ALL
    SELECT
        'POS_SALES',
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_POS_SALES
          WHERE _batch_date = '{{batch_date}}'),
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.FACT_POS_SALES
          WHERE _batch_date = '{{batch_date}}')

    UNION ALL
    SELECT
        'INVENTORY',
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_INVENTORY_SNAPSHOT
          WHERE _batch_date = '{{batch_date}}'),
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.FACT_INVENTORY_SNAPSHOT
          WHERE _batch_date = '{{batch_date}}')

    UNION ALL
    SELECT
        'PRICE',
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER
          WHERE _batch_date = '{{batch_date}}'),
        (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.FACT_PRICE
          WHERE _batch_date = '{{batch_date}}')

    UNION ALL
    SELECT
        'SALES_DAILY',
        (SELECT COALESCE(SUM(sales_qty), 0) FROM RETAIL_DW.RETAIL_SCHEMA.FACT_POS_SALES
          WHERE _batch_date = '{{batch_date}}'),
        (SELECT COALESCE(SUM(total_qty), 0) FROM RETAIL_DW.RETAIL_SCHEMA.RPT_SALES_DAILY
          WHERE _batch_date = '{{batch_date}}')
)
SELECT
    check_name,
    source_rows,
    target_rows,
    IFF(EQUAL_NULL(source_rows, target_rows), 'PASS', 'FAIL') AS result
FROM checks
ORDER BY check_name;
