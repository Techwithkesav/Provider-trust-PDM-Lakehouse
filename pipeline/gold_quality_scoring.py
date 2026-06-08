# pipeline/gold_quality_scoring.py
# Gold layer (part 2): score each golden provider record for directory
# accuracy/trustworthiness, flag specific issues, and produce a summary.

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pipeline.spark_session import get_spark
from config.settings import BUCKET, GOLD_PREFIX


def score():
    spark = get_spark("gold-quality-scoring")

    golden_path = f"s3a://{BUCKET}/{GOLD_PREFIX}/golden_providers"
    df = spark.read.parquet(golden_path)

    today = F.current_date()

    # 1. Compute individual problem flags (each is True when there's an issue).
    df = (df
          # license_expiry is stored as a string date; compare to today
          .withColumn("flag_license_expired",
                      F.to_date("license_expiry") < today)
          .withColumn("flag_missing_npi",
                      (F.col("npi").isNull()) | (F.trim(F.col("npi")) == ""))
          .withColumn("flag_single_source",
                      F.col("source_record_count") == 1)
          .withColumn("flag_incomplete_address",
                      (F.col("address").isNull()) | (F.trim(F.col("address")) == "") |
                      (F.col("state").isNull()) | (F.trim(F.col("state")) == "") |
                      (F.col("zip").isNull()) | (F.trim(F.col("zip")) == "")))

    # 2. Build an accuracy score: start at 100, subtract for each problem.
    #    Weights reflect business severity (expired license is worst).
    df = df.withColumn(
        "accuracy_score",
        F.lit(100)
        - F.when(F.col("flag_license_expired"), 40).otherwise(0)
        - F.when(F.col("flag_missing_npi"), 25).otherwise(0)
        - F.when(F.col("flag_incomplete_address"), 20).otherwise(0)
        - F.when(F.col("flag_single_source"), 10).otherwise(0)
    )

     # 3. Bucket into tiers a business audience understands.
    #    An expired license always forces ACTION_REQUIRED regardless of score,
    #    because a member must never be routed to an unlicensed provider.
    df = df.withColumn(
        "quality_tier",
        F.when(F.col("flag_license_expired"), "ACTION_REQUIRED")
         .when(F.col("accuracy_score") >= 90, "TRUSTED")
         .when(F.col("accuracy_score") >= 60, "REVIEW")
         .otherwise("ACTION_REQUIRED")
    )

    # 4. Build a human-readable list of issues per record (for a worklist).
    df = df.withColumn(
        "issues",
        F.concat_ws("; ",
            F.when(F.col("flag_license_expired"), F.lit("license expired")),
            F.when(F.col("flag_missing_npi"), F.lit("missing NPI")),
            F.when(F.col("flag_incomplete_address"), F.lit("incomplete address")),
            F.when(F.col("flag_single_source"), F.lit("unconfirmed (single source)")),
        )
    )

    # 5. Write the scored records (the care-team / steward worklist).
    out = f"s3a://{BUCKET}/{GOLD_PREFIX}/provider_accuracy"
    df.write.mode("overwrite").parquet(out)

    # 6. Build and write an executive summary table.
    total = df.count()
    summary = (df.groupBy("quality_tier")
               .agg(F.count("*").alias("provider_count"))
               .withColumn("pct_of_directory",
                           F.round(F.col("provider_count") / F.lit(total) * 100, 1)))

    summary_out = f"s3a://{BUCKET}/{GOLD_PREFIX}/directory_health_summary"
    summary.write.mode("overwrite").parquet(summary_out)

    # 7. Report to the console.
    avg_score = df.agg(F.round(F.avg("accuracy_score"), 1)).collect()[0][0]
    print(f"Total golden providers scored: {total}")
    print(f"Average directory accuracy score: {avg_score}/100\n")
    print("Directory health by tier:")
    summary.orderBy("quality_tier").show(truncate=False)
    print("Sample ACTION_REQUIRED records (the steward worklist):")
    (df.filter(F.col("quality_tier") == "ACTION_REQUIRED")
       .select("provider_name", "state", "accuracy_score", "issues")
       .show(8, truncate=False))

    print(f"Scored records written to:   {out}")
    print(f"Summary written to:          {summary_out}")
    spark.stop()


if __name__ == "__main__":
    score()