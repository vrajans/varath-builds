# Fabric notebook: 01_ingest_to_bronze_incremental
# Attach default lakehouse: lh_bronze
#
# Build #1 pulled EVERY record on EVERY run. This version asks each API only
# for records changed since the last successful load (the "watermark"), with
# three safety rules:
#   1. Lookback overlap   - re-read a small window so late commits aren't lost.
#   2. Watermark = max 'updated' value actually LOADED, never now().
#   3. Commit the watermark only AFTER bronze is written (failed run = retry).
# Duplicates from the overlap are harmless: silver MERGEs on a stable key.

# %% [parameters]
api_name = "lms_completions"
full_refresh = False          # True = ignore the watermark, reload everything
lookback_minutes = 10         # overlap window for late-arriving updates

# %%
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import pandas as pd
import requests
from pyspark.sql import types as T

# Watermarks are stored and compared in UTC (Fabric's default; set explicitly).
spark.conf.set("spark.sql.session.timeZone", "UTC")

RUN_ID = str(uuid.uuid4())
RUN_STARTED = datetime.now(timezone.utc)

cfg = (spark.table("lh_bronze.api_config")
       .where(f"api_name = '{api_name}' AND enabled").first())
if cfg is None:
    raise ValueError(f"No enabled config row for api_name={api_name}")


# %% ---------------------------------------------------------------- watermark
def read_watermark(api: str):
    if not spark.catalog.tableExists("lh_bronze.ingest_watermark"):
        return None
    row = (spark.table("lh_bronze.ingest_watermark")
           .where(f"api_name = '{api}'").first())
    return row.watermark_value.replace(tzinfo=timezone.utc) if row and row.watermark_value else None


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(value):
    """Parse the API's 'last updated' value (ISO 8601 string or epoch seconds)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    # pandas copes with every ISO variant APIs emit (Z, offsets, 7-digit fractions)
    return pd.to_datetime(str(value).strip(), utc=True).to_pydatetime()


def dig(record: dict, dotted: str):
    """Read a nested field such as 'fields.Modified'."""
    for part in dotted.split("."):
        if not isinstance(record, dict):
            return None
        record = record.get(part)
    return record


previous_wm = None if full_refresh else read_watermark(api_name)
since = previous_wm - timedelta(minutes=lookback_minutes) if previous_wm else None
print(f"{api_name}: previous watermark={previous_wm}  requesting since={since}")


# %% ---------------------------------------------------------------- HTTP (from build #1)
def get_session(cfg) -> requests.Session:
    secret = notebookutils.credentials.getSecret(cfg.key_vault_url, cfg.secret_name)
    s = requests.Session()
    if cfg.auth_type == "api_key":
        s.headers["x-api-key"] = secret
    elif cfg.auth_type == "oauth2_client_credentials":
        data = {"grant_type": "client_credentials", "client_id": cfg.client_id,
                "client_secret": secret}
        if cfg.oauth_scope:
            data["scope"] = cfg.oauth_scope
        tok = requests.post(cfg.token_url, data=data, timeout=60)
        tok.raise_for_status()
        s.headers["Authorization"] = f"Bearer {tok.json()['access_token']}"
    if "graph.microsoft.com" in cfg.base_url:
        s.headers["Prefer"] = "HonorNonIndexedQueriesWarningMayFailRandomly"
    return s


def _retry_after_seconds(resp, attempt: int) -> float:
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return float(ra)
        except ValueError:
            try:
                return max(0.0, (parsedate_to_datetime(ra) - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError):
                pass
    return 2 ** attempt + random.uniform(0, 1)


def get_with_retry(session, url, params=None, max_retries: int = 6):
    for attempt in range(max_retries):
        r = session.get(url, params=params, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(_retry_after_seconds(r, attempt), 120))
            continue
        r.raise_for_status()
        return r
    raise RuntimeError(f"Gave up after {max_retries} retries: {url}")


# %% ---------------------------------------------------------------- incremental paging
def fetch_pages(session, cfg, since):
    style = cfg.pagination_style
    url = cfg.base_url
    params = {cfg.page_size_param: cfg.page_size}
    if since is not None and cfg.incremental_param:
        params[cfg.incremental_param] = cfg.incremental_template.format(since=to_iso(since))
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
            url, params = body.get(cfg.next_link_field), None
            if not url:
                return
        offset += len(records)
        page += 1
        time.sleep(cfg.pause_seconds or 0)


# %% ---------------------------------------------------------------- land in bronze
schema = T.StructType([
    T.StructField("api_name", T.StringType()),
    T.StructField("run_id", T.StringType()),
    T.StructField("page_no", T.IntegerType()),
    T.StructField("record_count", T.IntegerType()),
    T.StructField("payload", T.StringType()),
    T.StructField("ingested_at", T.TimestampType()),
])

session = get_session(cfg)
buffer, total_records, max_seen = [], 0, None

def flush(rows):
    if rows:
        spark.createDataFrame(rows, schema).write.mode("append").saveAsTable("lh_bronze.api_raw")

for page_no, body in enumerate(fetch_pages(session, cfg, since), start=1):
    records = body.get(cfg.records_field) or []
    for rec in records:                                   # rule 2: track what we LOADED
        ts = parse_ts(dig(rec, cfg.record_updated_field)) if cfg.record_updated_field else None
        if ts and (max_seen is None or ts > max_seen):
            max_seen = ts
    buffer.append((cfg.api_name, RUN_ID, page_no, len(records), json.dumps(body),
                   datetime.now(timezone.utc)))
    total_records += len(records)
    if len(buffer) >= 50:
        flush(buffer)
        buffer = []
flush(buffer)

# %% ---------------------------------------------------------------- commit watermark (rule 3)
# Only reached if every bronze write above succeeded. Never move it backwards,
# and never set it to now(): rows updated while we were paging would be skipped.
new_wm = max(d for d in (previous_wm, max_seen) if d is not None) if (previous_wm or max_seen) else None

if new_wm is not None:
    wm_df = spark.createDataFrame(
        [(api_name, new_wm.replace(tzinfo=None), RUN_ID, datetime.utcnow())],
        "api_name STRING, watermark_value TIMESTAMP, last_run_id STRING, updated_at TIMESTAMP")
    if spark.catalog.tableExists("lh_bronze.ingest_watermark"):
        from delta.tables import DeltaTable
        (DeltaTable.forName(spark, "lh_bronze.ingest_watermark").alias("t")
         .merge(wm_df.alias("s"), "t.api_name = s.api_name")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        wm_df.write.saveAsTable("lh_bronze.ingest_watermark")

print(f"{api_name}: {total_records} records since {since}; watermark {previous_wm} -> {new_wm}")
notebookutils.notebook.exit(RUN_ID)
