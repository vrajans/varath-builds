-- Build #2: incremental loads.
-- Run in a notebook %%sql cell attached to lh_bronze, AFTER build #1 is in place.

-- 1. One row per API: how far we have successfully loaded.
CREATE TABLE IF NOT EXISTS lh_bronze.ingest_watermark (
    api_name         STRING,
    watermark_value  TIMESTAMP,   -- max 'last updated' value actually LOADED
    last_run_id      STRING,
    updated_at       TIMESTAMP
) USING DELTA;

-- 2. Teach each API config row how to ask for "only what changed".
--    incremental_param    : query parameter the API accepts
--    incremental_template : value to send; {since} is replaced with an ISO timestamp
--    record_updated_field : field in EACH record holding its last-modified time
ALTER TABLE lh_bronze.api_config ADD COLUMNS (
    incremental_param     STRING,
    incremental_template  STRING,
    record_updated_field  STRING
);

UPDATE lh_bronze.api_config
SET incremental_param = 'updated_since', incremental_template = '{since}',
    record_updated_field = 'updatedAt'
WHERE api_name = 'lms_completions';

UPDATE lh_bronze.api_config
SET incremental_param = 'modified_after', incremental_template = '{since}',
    record_updated_field = 'modified_ts'
WHERE api_name = 'internal_rest';

-- Microsoft Graph list items use OData $filter on the 'Modified' field.
-- Filtering on a non-indexed column needs the request header
--   Prefer: HonorNonIndexedQueriesWarningMayFailRandomly
-- (the ingest notebook adds it for Graph URLs) or an index on 'Modified' in
-- the SharePoint list settings. Test your filter in Graph Explorer first.
-- record_updated_field supports dotted paths for nested JSON.
UPDATE lh_bronze.api_config
SET incremental_param = '$filter',
    incremental_template = 'fields/Modified ge ''{since}''',
    record_updated_field = 'fields.Modified'
WHERE api_name = 'sharepoint_list';
