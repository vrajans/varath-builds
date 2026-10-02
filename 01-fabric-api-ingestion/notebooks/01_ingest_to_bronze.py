# Fabric notebook: 01_ingest_to_bronze
# Attach default lakehouse: lh_bronze
#
# Reads ONE row from lh_bronze.api_config (chosen by the api_name parameter),
# pages through the API whatever its pagination style, and appends every raw
# response to lh_bronze.api_raw. No parsing here — bronze keeps the raw JSON so
# an upstream schema change never loses data.

# %% [parameters]  <- mark this cell as the "Parameters" cell in Fabric
api_name = "lms_completions"

# %%
import json
import random
import time
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
from pyspark.sql import types as T

RUN_ID = str(uuid.uuid4())

cfg = (spark.table("lh_bronze.api_config")
       .where(f"api_name = '{api_name}' AND enabled")
       .first())
if cfg is None:
    raise ValueError(f"No enabled config row for api_name={api_name}")


# %% ---------------------------------------------------------------- auth
def get_session(cfg) -> requests.Session:
    """Build an authenticated session. Secrets come from Key Vault at run time."""
    secret = notebookutils.credentials.getSecret(cfg.key_vault_url, cfg.secret_name)
    s = requests.Session()
    if cfg.auth_type == "api_key":
        s.headers["x-api-key"] = secret
    elif cfg.auth_type == "oauth2_client_credentials":
        data = {"grant_type": "client_credentials",
                "client_id": cfg.client_id,
                "client_secret": secret}
        if cfg.oauth_scope:
            data["scope"] = cfg.oauth_scope
        tok = requests.post(cfg.token_url, data=data, timeout=60)
        tok.raise_for_status()
        s.headers["Authorization"] = f"Bearer {tok.json()['access_token']}"
    else:
        raise ValueError(f"Unknown auth_type {cfg.auth_type}")
    return s


# %% ---------------------------------------------------------------- the day-9 fix
def _retry_after_seconds(resp, attempt: int) -> float:
    """Honour Retry-After (seconds or HTTP date); otherwise exponential backoff."""
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return float(ra)
        except ValueError:
            try:
                dt = parsedate_to_datetime(ra)
                return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError):
                pass
    return 2 ** attempt + random.uniform(0, 1)   # 1s, 2s, 4s, 8s ... plus jitter


def get_with_retry(session, url, params=None, max_retries: int = 6):
    for attempt in range(max_retries):
        r = session.get(url, params=params, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            wait = min(_retry_after_seconds(r, attempt), 120)
            print(f"  HTTP {r.status_code} -> waiting {wait:.1f}s (attempt {attempt + 1})")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r
    raise RuntimeError(f"Gave up after {max_retries} retries: {url}")


# %% ---------------------------------------------------------------- pagination
def fetch_pages(session, cfg):
    """Yield raw JSON bodies, one per page, for any of 4 pagination styles."""
    style = cfg.pagination_style
    url = cfg.base_url
    params = {cfg.page_size_param: cfg.page_size}
    cursor, offset, page = None, 0, 1

    for _ in range(cfg.max_pages):
        if style == "cursor" and cursor:
            params[cfg.cursor_param] = cursor
        elif style == "offset":
            params[cfg.offset_param] = offset
        elif style == "page_number":
            params[cfg.page_param] = page

        body = get_with_retry(session, url, params).json()
        records = body.get(cfg.records_field) or []
        if not records:
            return
        yield body

        if style == "cursor":
            cursor = body.get(cfg.next_cursor_field)
            if not cursor:
                return
        elif style == "next_link":
            url = body.get(cfg.next_link_field)
            params = None            # next link already carries its query string
            if not url:
                return
        offset += len(records)
        page += 1
        time.sleep(cfg.pause_seconds or 0)   # stay under the rate limit


# %% ---------------------------------------------------------------- land in bronze
schema = T.StructType([
    T.StructField("api_name", T.StringType()),
    T.StructField("run_id", T.StringType()),
    T.StructField("page_no", T.IntegerType()),
    T.StructField("record_count", T.IntegerType()),
    T.StructField("payload", T.StringType()),          # raw JSON, untouched
    T.StructField("ingested_at", T.TimestampType()),
])

BATCH_PAGES = 50
session = get_session(cfg)
buffer, total_pages, total_records = [], 0, 0

for page_no, body in enumerate(fetch_pages(session, cfg), start=1):
    n = len(body.get(cfg.records_field) or [])
    buffer.append((cfg.api_name, RUN_ID, page_no, n, json.dumps(body),
                   datetime.now(timezone.utc)))
    total_pages, total_records = page_no, total_records + n
    if len(buffer) >= BATCH_PAGES:
        spark.createDataFrame(buffer, schema).write.mode("append").saveAsTable("lh_bronze.api_raw")
        buffer = []

if buffer:
    spark.createDataFrame(buffer, schema).write.mode("append").saveAsTable("lh_bronze.api_raw")

print(f"{api_name}: {total_pages} pages, {total_records} records, run_id={RUN_ID}")
notebookutils.notebook.exit(RUN_ID)   # handed back to the pipeline
