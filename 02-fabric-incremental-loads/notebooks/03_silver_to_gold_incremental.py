# Fabric notebook: 03_silver_to_gold_incremental
# Attach lakehouses: lh_silver (default), lh_gold
#
# Build #1 rebuilt the whole gold table every night. Now we only recompute the
# fiscal years that actually received new or changed rows, and overwrite just
# those partitions with Delta's replaceWhere. Untouched years are never read.

# %%
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
SILVER = "lh_silver.course_completions"
GOLD = "lh_gold.course_completion_summary"

silver = spark.table(SILVER)
gold_exists = spark.catalog.tableExists(GOLD)

# %% ---------------------------------------------------------------- what changed?
# Same rule as the ingest watermark: track the newest SOURCE row we processed
# (max silver _ingested_at), never the wall-clock time of this run.
source_wm = silver.agg(F.max("_ingested_at")).first()[0]

# Upgrading from build #1? Its gold table has no source_watermark yet, so the
# first run rebuilds everything once and adds the column.
if gold_exists and "source_watermark" not in spark.table(GOLD).columns:
    gold_exists = False

if gold_exists:
    last_wm = spark.table(GOLD).agg(F.max("source_watermark")).first()[0]
    changed_years = [r.fiscal_year for r in
                     silver.where(F.col("_ingested_at") > F.lit(last_wm))
                           .select("fiscal_year").distinct().collect()]
else:
    changed_years = None          # first run: build everything

if changed_years == []:
    notebookutils.notebook.exit("gold already up to date")

# %% ---------------------------------------------------------------- recompute only those years
scope = silver if changed_years is None else silver.where(F.col("fiscal_year").isin(changed_years))

gold = (scope
        .groupBy("partner_id", "fiscal_year")
        .agg(F.count("*").alias("completions"),
             F.countDistinct("learner_key").alias("unique_learners"),
             F.countDistinct("course_key").alias("courses_completed"),
             F.max("completed_at").alias("last_completion_at"))
        .withColumn("source_watermark", F.lit(source_wm).cast("timestamp"))
        .withColumn("refreshed_at", F.current_timestamp()))

writer = gold.write.mode("overwrite")
if changed_years is None:
    writer.option("overwriteSchema", "true").saveAsTable(GOLD)
else:
    in_list = ", ".join(f"'{y}'" for y in changed_years)
    writer.option("replaceWhere", f"fiscal_year IN ({in_list})").saveAsTable(GOLD)

print("gold refreshed for:", changed_years or "ALL fiscal years")
