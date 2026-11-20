-- item dimension (one row per item, SCD1).
-- Looks up subclass_sk from DIM_SUBCLASS, which must be loaded first.
MERGE INTO RETAIL_DW.RETAIL_SCHEMA.DIM_ITEM AS target
USING (
    SELECT
        HASH(i.item_id)     AS item_sk,
        i.item_id,
        i.item_name,
        i.subclass_id,
        sc.subclass_sk,
        i.subclass_name,
        i.unit_price,
        i.unit_cost,
        i.effective_from,
        i._batch_date,
        i._loaded_at
    FROM RETAIL_DW.RETAIL_SCHEMA.TMP_ITEM_MASTER i
    LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_SUBCLASS sc
           ON i.subclass_id = sc.subclass_id
    WHERE i._batch_date = '{{batch_date}}'
) AS source
ON target.item_id = source.item_id
WHEN MATCHED THEN
    UPDATE SET
        item_sk       = source.item_sk,
        item_name     = source.item_name,
        subclass_id   = source.subclass_id,
        subclass_sk   = source.subclass_sk,
        subclass_name = source.subclass_name,
        unit_price    = source.unit_price,
        unit_cost     = source.unit_cost,
        effective_from = source.effective_from,
        _batch_date   = source._batch_date,
        _loaded_at    = source._loaded_at
WHEN NOT MATCHED THEN
    INSERT (item_sk, item_id, item_name, subclass_id, subclass_sk, subclass_name,
            unit_price, unit_cost, effective_from, _batch_date, _loaded_at)
    VALUES (source.item_sk, source.item_id, source.item_name, source.subclass_id, source.subclass_sk,
            source.subclass_name, source.unit_price, source.unit_cost, source.effective_from,
            source._batch_date, source._loaded_at);
