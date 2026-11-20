-- mart: daily sales aggregate (day x location x item).
-- Rebuilt for the batch instead of merged, so a re-run never leaves
-- stale measures behind. Feeds the BI / finance dashboards.
DELETE FROM RETAIL_DW.RETAIL_SCHEMA.RPT_SALES_DAILY
WHERE date_sk IN (
    SELECT DISTINCT date_sk
    FROM RETAIL_DW.RETAIL_SCHEMA.FACT_POS_SALES
    WHERE _batch_date = '{{batch_date}}'
);

INSERT INTO RETAIL_DW.RETAIL_SCHEMA.RPT_SALES_DAILY (
    date_sk, full_date, location_sk, location_id, item_sk, item_id,
    line_count, transaction_count, total_qty, total_sales_amount,
    total_cost, gross_margin, _batch_date, _loaded_at
)
SELECT
    f.date_sk,
    dd.full_date,
    f.location_sk,
    f.location_id,
    f.item_sk,
    f.item_id,
    COUNT(*)                            AS line_count,
    COUNT(DISTINCT f.transaction_id)    AS transaction_count,
    SUM(f.sales_qty)                    AS total_qty,
    SUM(f.extended_amount)              AS total_sales_amount,
    SUM(f.sales_qty * di.unit_cost)     AS total_cost,
    SUM(f.extended_amount) - SUM(f.sales_qty * di.unit_cost) AS gross_margin,
    MAX(f._batch_date)                  AS _batch_date,
    CURRENT_TIMESTAMP()                 AS _loaded_at
FROM RETAIL_DW.RETAIL_SCHEMA.FACT_POS_SALES f
JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_ITEM di
  ON f.item_sk = di.item_sk
LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_DAY dd
  ON f.date_sk = dd.date_sk
WHERE f._batch_date = '{{batch_date}}'
GROUP BY f.date_sk, dd.full_date, f.location_sk, f.location_id, f.item_sk, f.item_id;
