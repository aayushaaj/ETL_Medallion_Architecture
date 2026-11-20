-- store dimension (one row per location, SCD1).
-- Looks up district_sk from DIM_DISTRICT, which must be loaded first.
MERGE INTO RETAIL_DW.RETAIL_SCHEMA.DIM_STORE AS target
USING (
    SELECT
        HASH(s.location_id)  AS location_sk,
        s.location_id,
        s.location_name,
        s.district_id,
        d.district_sk,
        s.district_name,
        s.region,
        s.effective_from,
        s._batch_date,
        s._loaded_at
    FROM RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER s
    LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_DISTRICT d
           ON s.district_id = d.district_id
    WHERE s._batch_date = '{{batch_date}}'
) AS source
ON target.location_id = source.location_id
WHEN MATCHED THEN
    UPDATE SET
        location_sk   = source.location_sk,
        location_name = source.location_name,
        district_id   = source.district_id,
        district_sk   = source.district_sk,
        district_name = source.district_name,
        region        = source.region,
        effective_from = source.effective_from,
        _batch_date   = source._batch_date,
        _loaded_at    = source._loaded_at
WHEN NOT MATCHED THEN
    INSERT (location_sk, location_id, location_name, district_id, district_sk,
            district_name, region, effective_from, _batch_date, _loaded_at)
    VALUES (source.location_sk, source.location_id, source.location_name, source.district_id,
            source.district_sk, source.district_name, source.region, source.effective_from,
            source._batch_date, source._loaded_at);
