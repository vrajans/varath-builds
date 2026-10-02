# Varath Builds It #1 — Metadata-driven REST API ingestion into Microsoft Fabric

Companion code for the YouTube video
**"How to Ingest REST APIs into Microsoft Fabric (Metadata-Driven, Step by Step)"**
on *Data & AI with Varath*.

Three REST APIs with three different pagination styles land in a Fabric lakehouse
(bronze → silver → gold) and feed Power BI through Direct Lake — with **one**
pipeline and **one** config table. Adding a new API is one row, not a new pipeline.

```
 REST APIs ──► 01_ingest_to_bronze ──► Bronze ──► Silver ──► Gold ──► Power BI
 (cursor,          ▲       ▲           raw JSON   typed,     KPI       Direct Lake
  offset,          │       │           append     keyed,     tables
  next-link)   api_config  Data Factory           deduped
               (1 row/API) (Lookup → ForEach → Notebook)
```

## What's in here

| Path | What it does |
|---|---|
| `config/api_config.sql` | The config table + 3 example rows. Secrets are Key Vault **names**, never values. |
| `notebooks/01_ingest_to_bronze.py` | Auth (API key / OAuth2 client credentials), 4 pagination styles, 429/Retry-After backoff, raw JSON append to bronze. |
| `notebooks/02_bronze_to_silver.py` | Parse, data-quality quarantine, course-ID + fiscal-year normalisation, SHA-256 stable keys, PII tokenisation, idempotent MERGE. |
| `notebooks/03_silver_to_gold.py` | One KPI table per business question, PII-free. |
| `sql/weekly_maintenance.sql` | OPTIMIZE + Z-ORDER + VACUUM to keep Direct Lake fast. |
| `pipeline/pipeline_setup.md` | How to wire it up in Fabric Data Factory. |

## Setup (≈30 minutes)

1. **Workspace + lakehouses:** create `lh_bronze`, `lh_silver`, `lh_gold`, and `lh_restricted`
   (PII mapping; restrict access to data stewards) in one Fabric workspace.
2. **Key Vault:** store each API secret; grant the workspace identity (or your user) *Get* on secrets.
3. **Config:** run `config/api_config.sql` in a notebook `%%sql` cell. Replace the example URLs,
   secret names and JSON field names with your APIs'.
4. **Notebooks:** import the three `.py` files as Fabric notebooks. In `01_ingest_to_bronze`,
   mark the first cell as the **Parameters** cell.
5. **Field mapping:** edit `FIELD_MAP` in `02_bronze_to_silver.py` to point at your JSON paths.
6. **Pipeline:** build it following `pipeline/pipeline_setup.md`. Run once manually, then schedule.
7. **Power BI:** create a Direct Lake semantic model on `lh_gold` only.

## Design decisions

- **Bronze stores raw JSON strings.** An upstream schema change can never lose data — silver
  fails loudly instead, and you can replay from bronze.
- **Stable composite key.** `course_key = SHA-256(partner_id | normalised course_id | fiscal year)`
  keeps year-on-year joins working when course-ID formats change between fiscal years.
- **Fiscal year starts 1 July** (FY25 = Jul 2024–Jun 2025). Change `fy_num` if yours differs.
- **Idempotent.** Silver uses MERGE on `completion_key`, so re-running a day never duplicates rows.
- **PII never reaches gold.** Emails become SHA-256 tokens in silver; the token→email map lives
  in `lh_restricted`. Apply a Purview sensitivity label to bronze and silver.

## Known limits

- Tokens are fetched once per run. For runs longer than the token lifetime (often 60 min),
  add a refresh-on-401 in `get_with_retry`.
- Pages are buffered 50 at a time on the driver — fine for millions of rows, not billions.
- Example API URLs and field names are placeholders; adapt them to your sources.

MIT licence (see [LICENSE](../LICENSE)) — use it, fork it, ship it.
More builds: [back to the series index](../README.md).
