-- Weekly maintenance (run in a notebook %%sql cell, or as a scheduled notebook).
-- Daily appends create many small Parquet files. After ~30 days Direct Lake
-- queries slowed from ~400 ms to ~3.8 s on the original project. This fixes it.

-- Compact small files and co-locate rows that are filtered together.
-- (Fabric also supports adding VORDER here for Power BI-friendly layout.)
OPTIMIZE lh_gold.course_completion_summary ZORDER BY (fiscal_year, partner_id);
OPTIMIZE lh_silver.course_completions ZORDER BY (fiscal_year, partner_id);
OPTIMIZE lh_bronze.api_raw;

-- Remove files no longer referenced. 168 hours = 7 days of time travel kept,
-- so a bad transformation can still be rolled back within a week.
VACUUM lh_gold.course_completion_summary RETAIN 168 HOURS;
VACUUM lh_silver.course_completions RETAIN 168 HOURS;
VACUUM lh_bronze.api_raw RETAIN 168 HOURS;
