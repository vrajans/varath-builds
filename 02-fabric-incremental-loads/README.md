# Varath Builds It #2 — Incremental loads in Microsoft Fabric

Companion code for **"Incremental Loads in Microsoft Fabric (Stop Reloading Everything)"**
on *Data & AI with Varath*. Builds directly on [#1 REST API ingestion](../01-fabric-api-ingestion/).

Build #1 pulled every record from every API on every run. That's fine on day one,
but the run time, API calls and Fabric capacity it burns grow with your history,
forever. This build loads only what changed — without silently losing updates.

## The three rules

| Rule | Why |
|---|---|
| **1. Re-read a small overlap** (`lookback_minutes`, default 10) | Records committed late, or clocks slightly out of sync, would otherwise fall into a gap. Duplicates from the overlap are harmless because silver MERGEs on a stable key. |
| **2. Watermark = the newest `updated` value you actually loaded — never `now()`** | Rows changed while the run was paging have an update time *before* `now()` but weren't in your pages. Setting the watermark to `now()` skips them forever, with no error. |
| **3. Commit the watermark only after bronze is written** | If the run fails halfway, the watermark doesn't move, so the next run retries the same window. |

## What's in here

| Path | What it does |
|---|---|
| `sql/ingest_watermark.sql` | Creates `lh_bronze.ingest_watermark` and adds three incremental columns to `api_config`. |
| `notebooks/01_ingest_to_bronze_incremental.py` | Replaces build #1's ingest notebook: watermark read → `since` filter → paging → bronze → watermark commit. |
| `notebooks/03_silver_to_gold_incremental.py` | Recomputes only the fiscal years that changed, using Delta `replaceWhere`. |

`02_bronze_to_silver.py` from build #1 is **unchanged**: it was already incremental
(reads bronze rows newer than silver's `_ingested_at`) and idempotent (MERGE).

## Upgrade steps (from build #1)

1. Run `sql/ingest_watermark.sql`, then edit the three `UPDATE` statements to match how
   **your** APIs filter by modified date (check each API's docs; test Graph filters in
   Graph Explorer).
2. Swap the pipeline's ingest Notebook activity to `01_ingest_to_bronze_incremental`.
   Add base parameters `full_refresh` (default `false`) and `lookback_minutes` (default `10`).
3. Swap the gold Notebook activity to `03_silver_to_gold_incremental`. Its first run
   rebuilds gold once to add the `source_watermark` column, then it goes incremental.
4. Run once manually. The first ingest run has no watermark, so it loads everything;
   from the second run on it's incremental.

## Backfills and resets

- Reload one API completely: run the ingest notebook with `full_refresh = true`.
- Re-load from a specific date: `UPDATE lh_bronze.ingest_watermark SET watermark_value = '2026-01-01' WHERE api_name = '...'`.

## Known limit: hard deletes

Filtering by "modified since" never sees records that were **deleted** at the source.
If deletes matter, either use the API's deleted-items endpoint (Graph has delta queries),
or schedule a weekly full-key reconciliation that soft-deletes keys missing from the source.

MIT licence (see [LICENSE](../LICENSE)). More builds: [series index](../README.md).
