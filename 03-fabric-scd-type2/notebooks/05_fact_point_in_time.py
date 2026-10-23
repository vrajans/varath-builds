# Fabric notebook: 05_fact_point_in_time
# Attach lakehouses: lh_silver (default), lh_gold
#
# Power BI relationships can only match EQUAL keys. They cannot express
# "completed_at BETWEEN valid_from AND valid_to". So we resolve the partner
# version here, in gold, and store partner_sk on every fact row. Power BI then
# joins fact.partner_sk = dim_partner.partner_sk, and every number shows the
# region / tier the partner had AT THE TIME of the completion.

# %%
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

fact = spark.table("lh_silver.course_completions").alias("f")
dim = spark.table("lh_gold.dim_partner").alias("d")

joined = fact.join(
    dim,
    (F.col("f.partner_id") == F.col("d.partner_id"))
    & (F.col("f.completed_at") >= F.col("d.valid_from"))   # valid_from inclusive
    & (F.col("f.completed_at") < F.col("d.valid_to")),     # valid_to exclusive
    "left")

# Unmatched rows (partner not in the master yet) stay visible as 'Unknown'
# instead of silently disappearing from the totals.
gold = (joined
        .groupBy(F.col("d.partner_sk"), F.col("f.partner_id"), F.col("f.fiscal_year"))
        .agg(F.count("*").alias("completions"),
             F.countDistinct("f.learner_key").alias("unique_learners"),
             F.max("f.completed_at").alias("last_completion_at"))
        .withColumn("partner_sk", F.coalesce("partner_sk", F.lit(-1).cast("bigint")))
        .withColumn("refreshed_at", F.current_timestamp()))

(gold.write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable("lh_gold.fact_completions_by_partner"))

unknown = gold.where("partner_sk = -1").agg(F.sum("completions")).first()[0] or 0
print(f"fact rows: {gold.count()}  |  completions with unknown partner: {unknown}")
