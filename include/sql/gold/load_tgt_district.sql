MERGE INTO RETAIL_DW.RETAIL_SCHEMA.DIM_DISTRICT AS target
USING (
    SELECT
        HASH(district_id)      AS district_sk,
        district_id,
        MAX(district_name)     AS district_name,
        MAX(region)            AS region,
        MAX(_batch_date)       AS _batch_date,
        MAX(_loaded_at)        AS _loaded_at
    FROM RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER
    WHERE _batch_date = '{{batch_date}}'
      AND district_id IS NOT NULL
      AND TRIM(district_id) <> ''
    GROUP BY district_id
) AS source
ON target.district_id = source.district_id
WHEN MATCHED THEN
    UPDATE SET
        district_sk   = source.district_sk,
        district_name = source.district_name,
        region        = source.region,
        _batch_date   = source._batch_date,
        _loaded_at    = source._loaded_at
WHEN NOT MATCHED THEN
    INSERT (district_sk, district_id, district_name, region, _batch_date, _loaded_at)
    VALUES (source.district_sk, source.district_id, source.district_name, source.region, source._batch_date, source._loaded_at);
