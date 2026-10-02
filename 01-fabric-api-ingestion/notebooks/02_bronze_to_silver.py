# Fabric notebook: 02_bronze_to_silver
# Attach lakehouses: lh_bronze (default), lh_silver, lh_restricted
#
# Parses raw JSON from bronze, applies data-quality rules, normalises course IDs
# and fiscal years, builds stable hash keys, tokenises PII, and MERGEs into
# silver so re-runs never create duplicates.

# %%
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

SILVER = "lh_silver.course_completions"
QUARANTINE = "lh_silver.course_completions_quarantine"
PII_MAP = "lh_restricted.learner_key_map"      # data stewards only

# JSON paths per source. A new API = one config row + one entry here.
FIELD_MAP = {
    "lms_completions": {
        "partner_id": "$.partnerId",
        "course_id": "$.courseCode",
        "learner_email": "$.user.email",
        "completed_at": "$.completedDate",
    },
    "internal_rest": {
        "partner_id": "$.partner_id",
        "course_id": "$.course_id",
        "learner_email": "$.email",
        "completed_at": "$.completion_ts",
    },
    "sharepoint_list": {
        "partner_id": "$.fields.PartnerId",
        "course_id": "$.fields.CourseId",
        "learner_email": "$.fields.LearnerEmail",
        "completed_at": "$.fields.CompletedOn",
    },
}

# %% ---------------------------------------------------------------- incremental read
silver_exists = spark.catalog.tableExists(SILVER)
watermark = (spark.table(SILVER).agg(F.max("_ingested_at")).first()[0]
             if silver_exists else None)

cfgs = (spark.table("lh_bronze.api_config").where("enabled")
        .select("api_name", "records_field").collect())

frames = []
for c in cfgs:
    paths = FIELD_MAP[c.api_name]
    raw = spark.table("lh_bronze.api_raw").where(F.col("api_name") == c.api_name)
    if watermark is not None:
        raw = raw.where(F.col("ingested_at") > F.lit(watermark))
    # explode the records array; each record stays a JSON string
    recs = raw.select(
        "api_name", "ingested_at",
        F.explode(F.from_json(F.get_json_object("payload", f"$.{c.records_field}"),
                              T.ArrayType(T.StringType()))).alias("rec"))
    frames.append(recs.select(
        F.col("api_name").alias("source"),
        F.col("ingested_at").alias("_ingested_at"),
        *[F.get_json_object("rec", p).alias(col) for col, p in paths.items()]))

if not frames:
    notebookutils.notebook.exit("no sources")
df = reduce(DataFrame.unionByName, frames)

# %% ---------------------------------------------------------------- data quality
df = df.withColumn("completed_at", F.to_timestamp("completed_at"))
is_valid = (F.col("partner_id").isNotNull() & F.col("course_id").isNotNull()
            & F.col("learner_email").isNotNull() & F.col("completed_at").isNotNull()
            & (F.col("completed_at") <= F.current_timestamp()))
df.where(~is_valid).write.mode("append").saveAsTable(QUARANTINE)
df = df.where(is_valid)

# %% ---------------------------------------------------------------- normalise + keys
fy_num = F.year("completed_at") + F.when(F.month("completed_at") >= 7, 1).otherwise(0)

df = (df
      # 'CRS-24-1023' and 'C-25-1023' are the same course -> '1023'
      .withColumn("course_id_norm", F.regexp_extract("course_id", r"(\d+)$", 1))
      .withColumn("fiscal_year", F.concat(F.lit("FY"),
                                          F.lpad((fy_num % 100).cast("string"), 2, "0")))
      # PII: email becomes an irreversible SHA-256 token
      .withColumn("learner_key", F.sha2(F.lower(F.trim("learner_email")), 256))
      # the stable composite key that survives course-ID format changes
      .withColumn("course_key", F.sha2(F.concat_ws("|", "partner_id",
                                                   "course_id_norm", "fiscal_year"), 256))
      .withColumn("completion_key", F.sha2(F.concat_ws("|", "course_key", "learner_key"), 256)))

# token -> email mapping lives in a separate, restricted lakehouse
pii = df.select("learner_key", F.lower(F.trim("learner_email")).alias("learner_email")).distinct()
if spark.catalog.tableExists(PII_MAP):
    (DeltaTable.forName(spark, PII_MAP).alias("t")
     .merge(pii.alias("s"), "t.learner_key = s.learner_key")
     .whenNotMatchedInsertAll().execute())
else:
    pii.write.saveAsTable(PII_MAP)

df = df.drop("learner_email")

# latest version of each completion wins
w = Window.partitionBy("completion_key").orderBy(F.col("_ingested_at").desc())
df = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

# %% ---------------------------------------------------------------- merge into silver
if silver_exists:
    (DeltaTable.forName(spark, SILVER).alias("t")
     .merge(df.alias("s"), "t.completion_key = s.completion_key")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
else:
    df.write.saveAsTable(SILVER)

print("silver rows:", spark.table(SILVER).count())
