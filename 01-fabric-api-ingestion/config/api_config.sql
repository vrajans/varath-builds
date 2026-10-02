-- api_config: one row per source API.
-- Run in a Fabric notebook (%%sql cell) attached to the lh_bronze lakehouse.
-- Adding a new API = inserting one row here. No pipeline changes.
--
-- Secrets are NEVER stored here — only the Key Vault secret NAME.

CREATE TABLE IF NOT EXISTS lh_bronze.api_config (
    api_name            STRING,   -- unique key, also the ForEach parameter
    enabled             BOOLEAN,
    base_url            STRING,
    auth_type           STRING,   -- 'api_key' | 'oauth2_client_credentials'
    token_url           STRING,   -- oauth2 only
    key_vault_url       STRING,   -- https://<vault>.vault.azure.net/
    secret_name         STRING,   -- api key, or client secret for oauth2
    client_id           STRING,   -- oauth2 only
    oauth_scope         STRING,   -- oauth2 only, e.g. https://graph.microsoft.com/.default
    pagination_style    STRING,   -- 'cursor' | 'offset' | 'page_number' | 'next_link'
    records_field       STRING,   -- JSON field that holds the list of records
    page_size           INT,
    page_size_param     STRING,
    cursor_param        STRING,   -- cursor style: query param to send the cursor in
    next_cursor_field   STRING,   -- cursor style: response field holding next cursor
    offset_param        STRING,   -- offset style
    page_param          STRING,   -- page_number style
    next_link_field     STRING,   -- next_link style (e.g. '@odata.nextLink')
    pause_seconds       DOUBLE,   -- politeness delay between pages (rate limits)
    max_pages           INT       -- safety stop
) USING DELTA;

INSERT INTO lh_bronze.api_config VALUES
 ('lms_completions', true, 'https://lms.example.com/api/v2/completions',
  'oauth2_client_credentials', 'https://lms.example.com/oauth/token',
  'https://kv-dataplatform.vault.azure.net/', 'lms-client-secret', 'fabric-ingest', NULL,
  'cursor', 'data', 500, 'limit', 'cursor', 'next_cursor', NULL, NULL, NULL, 0.2, 5000),

 ('internal_rest', true, 'https://internal.example.com/api/completions',
  'api_key', NULL,
  'https://kv-dataplatform.vault.azure.net/', 'internal-api-key', NULL, NULL,
  'offset', 'items', 1000, 'limit', NULL, NULL, 'offset', NULL, NULL, 0.6, 5000),

 ('sharepoint_list', true, 'https://graph.microsoft.com/v1.0/sites/<site-id>/lists/<list-id>/items?expand=fields',
  'oauth2_client_credentials', 'https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token',
  'https://kv-dataplatform.vault.azure.net/', 'graph-client-secret', '<app-client-id>', 'https://graph.microsoft.com/.default',
  'next_link', 'value', 200, '$top', NULL, NULL, NULL, NULL, '@odata.nextLink', 0.6, 5000);
