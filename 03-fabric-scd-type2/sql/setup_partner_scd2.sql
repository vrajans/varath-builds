-- Build #3: SCD Type 2 partner dimension.
-- Run in a notebook %%sql cell. Assumes builds #1 and #2 are in place.

-- 1. The partner master list becomes source #4. Episode 1's promise:
--    a new API is ONE config row, no pipeline change.
--    Columns in table order (build #1's 20 + build #2's 3).
INSERT INTO lh_bronze.api_config VALUES (
   'partner_master',                                   -- api_name
   true,                                               -- enabled
   'https://graph.microsoft.com/v1.0/sites/<site-id>/lists/<partners-list-id>/items?expand=fields',
   'oauth2_client_credentials',                        -- auth_type
   'https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token',
   'https://kv-dataplatform.vault.azure.net/',         -- key_vault_url
   'graph-client-secret',                              -- secret_name
   '<app-client-id>',                                  -- client_id
   'https://graph.microsoft.com/.default',             -- oauth_scope
   'next_link',                                        -- pagination_style
   'value',                                            -- records_field
   200,                                                -- page_size
   '$top',                                             -- page_size_param
   NULL, NULL, NULL, NULL,                             -- cursor/offset/page params (unused)
   '@odata.nextLink',                                  -- next_link_field
   0.6,                                                -- pause_seconds
   5000,                                               -- max_pages
   '$filter',                                          -- incremental_param
   'fields/Modified ge ''{since}''',                   -- incremental_template
   'fields.Modified'                                   -- record_updated_field
);

-- 2. The SCD Type 2 dimension.
--    One row per partner VERSION. Exactly one row per partner has is_current = true.
CREATE TABLE IF NOT EXISTS lh_gold.dim_partner (
    partner_sk           BIGINT,      -- surrogate key: one per version (facts join on this)
    partner_id           STRING,      -- business key: same across versions
    partner_name         STRING,      -- Type 1: corrected in place on every version
    region               STRING,      -- Type 2: change = new version
    tier                 STRING,      -- Type 2
    account_manager      STRING,      -- Type 2
    row_hash             STRING,      -- SHA-256 of the Type 2 columns
    valid_from           TIMESTAMP,   -- inclusive
    valid_to             TIMESTAMP,   -- exclusive; 9999-12-31 for the current version
    is_current           BOOLEAN,
    _source_ingested_at  TIMESTAMP    -- bronze watermark for incremental runs
) USING DELTA;

-- 3. 'Unknown' member, so facts whose partner isn't in the master yet still
--    join in Power BI (fact.partner_sk = -1) instead of vanishing from totals.
INSERT INTO lh_gold.dim_partner VALUES
  (-1, '-1', 'Unknown partner', 'Unknown', 'Unknown', 'Unknown', NULL,
   TIMESTAMP'1900-01-01', TIMESTAMP'9999-12-31', false, NULL);
