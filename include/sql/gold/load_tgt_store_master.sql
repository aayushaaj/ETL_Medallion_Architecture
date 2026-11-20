-- store dimension (SCD Type 2: historical tracking for location changes, district moves).
-- Looks up district_sk from DIM_DISTRICT, which must be loaded first.

-- Step 1: Expire previous active version when tracked attributes change
UPDATE RETAIL_DW.RETAIL_SCHEMA.DIM_STORE AS target
SET effective_to = source.effective_from,
    is_current   = FALSE,
    _loaded_at   = CURRENT_TIMESTAMP(),
    _updated_at  = CURRENT_TIMESTAMP()
FROM (
    SELECT s.location_id, s.effective_from, s.location_name, s.district_id, s.region
    FROM RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER s
    WHERE s._batch_date = '{{batch_date}}'
) AS source
WHERE target.location_id = source.location_id
  AND target.is_current = TRUE
  AND target.effective_from < source.effective_from
  AND (
      COALESCE(target.district_id, '') <> COALESCE(source.district_id, '')
      OR COALESCE(target.location_name, '') <> COALESCE(source.location_name, '')
      OR COALESCE(target.region, '') <> COALESCE(source.region, '')
  );

-- Step 2: Merge incoming version (keyed on surrogate key location_id + effective_from)
MERGE INTO RETAIL_DW.RETAIL_SCHEMA.DIM_STORE AS target
USING (
    SELECT
        HASH(s.location_id, s.effective_from) AS location_sk,
        s.location_id,
        s.location_name,
        s.district_id,
        d.district_sk,
        s.district_name,
        s.region,
        s.effective_from,
        TO_DATE('9999-12-31')                 AS effective_to,
        TRUE                                  AS is_current,
        s._batch_date,
        s._loaded_at
    FROM RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER s
    LEFT JOIN RETAIL_DW.RETAIL_SCHEMA.DIM_DISTRICT d
           ON s.district_id = d.district_id
    WHERE s._batch_date = '{{batch_date}}'
) AS source
ON target.location_sk = source.location_sk
WHEN MATCHED THEN
    UPDATE SET
        location_name = source.location_name,
        district_id   = source.district_id,
        district_sk   = source.district_sk,
        district_name = source.district_name,
        region        = source.region,
        _batch_date   = source._batch_date,
        _loaded_at    = CURRENT_TIMESTAMP(),
        _updated_at   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (location_sk, location_id, location_name, district_id, district_sk,
            district_name, region, effective_from, effective_to, is_current, _batch_date, _loaded_at, _inserted_at, _updated_at)
    VALUES (source.location_sk, source.location_id, source.location_name, source.district_id,
            source.district_sk, source.district_name, source.region, source.effective_from,
            source.effective_to, source.is_current, source._batch_date, source._loaded_at, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- Step 3: Audit log
UPDATE RETAIL_DW.RETAIL_SCHEMA.C_BATCH_LOG
SET status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    rows_read = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.TMP_STORE_MASTER WHERE _batch_date = '{{batch_date}}'),
    rows_inserted = (SELECT COUNT(*) FROM RETAIL_DW.RETAIL_SCHEMA.DIM_STORE WHERE _batch_date = '{{batch_date}}'),
    rows_rejected = 0
WHERE run_id = '{{run_id}}' AND _batch_date = '{{batch_date}}' AND script_name = '{{script_name}}';
