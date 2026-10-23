# Fabric notebook: 04_dim_partner_scd2
# Attach lakehouses: lh_bronze (default), lh_gold
#
# Keeps full history of partner changes (SCD Type 2) with ONE atomic Delta MERGE:
#   - changed partner  -> close the current version + insert a new one
#   - new partner      -> insert its first version
#   - unchanged        -> nothing
# Type 1 columns (partner_name) are corrected in place on every version.

# %%
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.conf.set("spark.sql.session.timeZone", "UTC")

DIM = "lh_gold.dim_partner"
TYPE2_COLS = ["region", "tier", "account_manager"]   # a change = a new version
TYPE1_COLS = ["partner_name"]                         # a change = fix in place
LOW_DATE = "1900-01-01 00:00:00"
HIGH_DATE = "9999-12-31 00:00:00"

# %% ---------------------------------------------------------------- 1. read new partner rows
wm = spark.table(DIM).agg(F.max("_source_ingested_at")).first()[0]
raw = spark.table("lh_bronze.api_raw").where("api_name = 'partner_master'")
if wm is not None:
    raw = raw.where(F.col("ingested_at") > F.lit(wm))

recs = raw.select(
    "ingested_at",
    F.explode(F.from_json(F.get_json_object("payload", "$.value"),
                          T.ArrayType(T.StringType()))).alias("rec"))

src = recs.select(
    F.get_json_object("rec", "$.fields.PartnerId").alias("partner_id"),
    F.get_json_object("rec", "$.fields.PartnerName").alias("partner_name"),
    F.get_json_object("rec", "$.fields.Region").alias("region"),
    F.get_json_object("rec", "$.fields.Tier").alias("tier"),
    F.get_json_object("rec", "$.fields.AccountManager").alias("account_manager"),
    F.to_timestamp(F.get_json_object("rec", "$.fields.Modified")).alias("effective_from"),
    F.col("ingested_at").alias("_source_ingested_at"),
).where(F.col("partner_id").isNotNull())

# latest version of each partner in this batch
w = Window.partitionBy("partner_id").orderBy(F.col("effective_from").desc(),
                                             F.col("_source_ingested_at").desc())
src = src.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

if src.isEmpty():
    notebookutils.notebook.exit("no partner changes")

# %% ---------------------------------------------------------------- 2. detect change with a hash
# coalesce first: concat_ws skips NULLs, so ('A', NULL) and (NULL, 'A') would collide
src = src.withColumn("row_hash", F.sha2(F.concat_ws(
    "|", *[F.coalesce(F.col(c), F.lit("<null>")) for c in TYPE2_COLS]), 256))

# %% ---------------------------------------------------------------- 3. stage rows for one MERGE
current = spark.table(DIM).where("is_current")

# partners whose Type 2 columns changed, and the change is NEWER than the
# current version (an out-of-order older record must not reopen history)
changed = (src.alias("s")
           .join(current.alias("t"), F.col("s.partner_id") == F.col("t.partner_id"))
           .where("s.row_hash <> t.row_hash AND s.effective_from > t.valid_from")
           .select("s.*"))

# The trick: every incoming row once WITH its key (matches -> can close the
# current version), plus each changed row again with a NULL key (never matches
# -> gets inserted as the new version). One MERGE, one atomic commit.
staged = (src.withColumn("merge_key", F.col("partner_id"))
             .withColumn("_new_version", F.lit(False))
          .unionByName(changed.withColumn("merge_key", F.lit(None).cast("string"))
                              .withColumn("_new_version", F.lit(True))))

staged = (staged
          .withColumn("valid_from", F.when(F.col("_new_version"), F.col("effective_from"))
                                     .otherwise(F.lit(LOW_DATE).cast("timestamp")))
          .withColumn("partner_sk", F.xxhash64("partner_id", "valid_from")))

# %% ---------------------------------------------------------------- 4. the SCD2 MERGE
(DeltaTable.forName(spark, DIM).alias("t")
 .merge(staged.alias("s"), "t.partner_id = s.merge_key AND t.is_current = true")
 .whenMatchedUpdate(                                     # close the old version
     condition="t.row_hash <> s.row_hash AND s.effective_from > t.valid_from",
     set={"is_current": "false", "valid_to": "s.effective_from"})
 .whenNotMatchedInsert(values={                          # new partner OR new version
     "partner_sk": "s.partner_sk",
     "partner_id": "s.partner_id",
     "partner_name": "s.partner_name",
     "region": "s.region",
     "tier": "s.tier",
     "account_manager": "s.account_manager",
     "row_hash": "s.row_hash",
     "valid_from": "s.valid_from",
     "valid_to": f"CAST('{HIGH_DATE}' AS TIMESTAMP)",
     "is_current": "true",
     "_source_ingested_at": "s._source_ingested_at"})
 .execute())

# %% ---------------------------------------------------------------- 5. Type 1 columns: fix every version
(DeltaTable.forName(spark, DIM).alias("t")
 .merge(src.alias("s"), "t.partner_id = s.partner_id")
 .whenMatchedUpdate(
     condition=" OR ".join(f"NOT (t.{c} <=> s.{c})" for c in TYPE1_COLS),
     set={c: f"s.{c}" for c in TYPE1_COLS})
 .execute())

print("versions:", spark.table(DIM).count(),
      "| current partners:", spark.table(DIM).where("is_current").count())
