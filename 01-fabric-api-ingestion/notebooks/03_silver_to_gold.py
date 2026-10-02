# Fabric notebook: 03_silver_to_gold
# Attach lakehouses: lh_silver (default), lh_gold
#
# Gold is small and boring on purpose: one table per business question.
# Power BI reads ONLY this layer (Direct Lake). No PII reaches here.

# %%
from pyspark.sql import functions as F

silver = spark.table("lh_silver.course_completions")

gold = (silver
        .groupBy("partner_id", "fiscal_year")
        .agg(F.count("*").alias("completions"),
             F.countDistinct("learner_key").alias("unique_learners"),
             F.countDistinct("course_key").alias("courses_completed"),
             F.max("completed_at").alias("last_completion_at"))
        .withColumn("refreshed_at", F.current_timestamp()))

(gold.write
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable("lh_gold.course_completion_summary"))

print("gold rows:", gold.count())
