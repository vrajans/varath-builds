# Fabric Data Factory pipeline: `pl_api_to_gold`

Build this in the Fabric UI (Data Factory → New pipeline). It is deliberately tiny:
the logic lives in the notebooks and the config table, not in the pipeline.

```
[Lookup: lkp_api_config]
        │  query the enabled rows of lh_bronze.api_config
        ▼
[ForEach: each_api]            Items: @activity('lkp_api_config').output.value
        │                      Sequential: OFF  (batch count 3 — one per API)
        └─► [Notebook: 01_ingest_to_bronze]
                Base parameter  api_name = @item().api_name
        ▼
[Notebook: 02_bronze_to_silver]     (runs once, after all APIs land)
        ▼
[Notebook: 03_silver_to_gold]
        ▼
[If Condition: is_sunday]     @equals(dayOfWeek(utcNow()), 0)
        └─► True: [Notebook: weekly_maintenance]   (runs sql/weekly_maintenance.sql)
```

## Settings that matter

| Setting | Value | Why |
|---|---|---|
| Lookup source | Lakehouse `lh_bronze`, table `api_config`, filter `enabled = true` | One row per API drives the ForEach |
| ForEach batch count | 3 (or number of APIs) | APIs ingest in parallel; each has its own rate limit |
| Notebook activity retry | 1, interval 300 s | Absorbs a transient outage without paging anyone |
| Schedule | Daily, 02:00 UTC | Gold is fresh before the working day |
| Failure alert | Add an Office 365 Outlook / Teams activity on the failure path | Team knows within minutes |

## Adding a new API

1. Insert one row into `lh_bronze.api_config` (see `config/api_config.sql`).
2. Add its JSON field paths to `FIELD_MAP` in `02_bronze_to_silver.py`.
3. Done. The pipeline picks it up on the next run.
