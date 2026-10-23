# Varath Builds It #3 — SCD Type 2 in Microsoft Fabric (PySpark + Delta MERGE)

Companion code for **"How to Build SCD Type 2 in Microsoft Fabric (Step by Step)"**
on *Data & AI with Varath*. Builds on [#1](../01-fabric-api-ingestion/) and
[#2](../02-fabric-incremental-loads/).

**The problem:** a partner moves from EMEA to APAC. If the dimension just overwrites
the region, every *past* completion silently moves to APAC too — last quarter's
report changes overnight and nobody touched it. SCD Type 2 keeps a new row per
version, so every number shows the region the partner had **at the time**.

## What's in here

| Path | What it does |
|---|---|
| `sql/setup_partner_scd2.sql` | Adds the partner master as source #4 (one config row, as promised in #1), creates `lh_gold.dim_partner`, adds the `-1` Unknown member. |
| `notebooks/04_dim_partner_scd2.py` | Incremental read from bronze → row hash → **one atomic MERGE** that closes changed versions and inserts new ones. Type 1 fix-in-place for `partner_name`. |
| `notebooks/05_fact_point_in_time.py` | Resolves the right partner version per completion (`valid_from <= completed_at < valid_to`) and stores `partner_sk` on the fact. |
| `tests/test_scd2_merge.py` | Runs the same MERGE logic on a local Delta table with delta-rs. No Fabric needed. |

## How the one-MERGE trick works

Each incoming partner row is staged **twice** if it changed:

| Staged row | `merge_key` | What MERGE does |
|---|---|---|
| Every incoming row | `partner_id` | Matches the current version → **closes it** (`is_current = false`, `valid_to = change time`) if the hash changed |
| Changed rows again | `NULL` | Never matches → **inserted** as the new current version |
| Brand-new partners | `partner_id` | No current version → inserted as version 1 |

Both halves land in a single Delta commit, so Power BI never sees a partner with
zero or two current versions.

## Design decisions

- **Change detection by hash** of the Type 2 columns (`region`, `tier`, `account_manager`),
  with NULLs coalesced first so `('A', NULL)` and `(NULL, 'A')` don't collide.
- **Out-of-order safety:** a change only applies if it's newer than the current version's
  `valid_from`. A late, older record can't reopen history.
- **First version starts at 1900-01-01**, so facts older than the first time we saw a
  partner still find a version.
- **`valid_to` is exclusive**; the current version ends at 9999-12-31.
- **Surrogate key** `partner_sk = xxhash64(partner_id, valid_from)`: deterministic, so a
  rerun produces the same keys.
- **Why the join happens in gold:** Power BI relationships only match equal keys; they can't
  do "between valid_from and valid_to". Resolve the version once in gold, relate
  `fact.partner_sk = dim_partner.partner_sk`, and every visual is historically correct.
  For "current region" reporting, add a second relationship on `partner_id` to a view of
  `dim_partner WHERE is_current`.

## Setup

1. Run `sql/setup_partner_scd2.sql` (edit the Graph site/list IDs and field names).
2. The pipeline from #1/#2 now ingests `partner_master` automatically (new config row).
3. Add notebooks `04_dim_partner_scd2` then `05_fact_point_in_time` after silver in the pipeline.
4. Power BI (Direct Lake on `lh_gold`): relate `fact_completions_by_partner.partner_sk` →
   `dim_partner.partner_sk`.

## Known limits

- Several changes to one partner inside a single run collapse to the latest. Fine for a
  daily load; if you need every intermediate version, process bronze rows in time order.
- Partners deleted at the source stay current. Add an `is_deleted` flag from a periodic
  full-key reconciliation (see #2's README).

MIT licence (see [LICENSE](../LICENSE)). More builds: [series index](../README.md).
