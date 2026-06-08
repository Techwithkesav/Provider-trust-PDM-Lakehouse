# pipeline/gold_entity_resolution.py
# Gold layer (part 1): entity resolution.
# Group clean Silver records that refer to the same real provider, then
# merge each group into a single trustworthy "golden record".

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F, Window
from pipeline.spark_session import get_spark
from config.settings import BUCKET, SILVER_PREFIX, GOLD_PREFIX


def build_match_key(df):
    """Assigns each record a match_key identifying the same provider.

    Matching strategy, strongest signal first:
      1. If an NPI exists -> use it (authoritative national identifier).
      2. Otherwise (NPI missing) -> use a FUZZY key built from the phonetic
         code of the last name + state. Soundex encodes names by sound, so
         'braun'/'brown' and 'c braun'/'cheryl braun' (same last name, same
         state) collapse together. We restrict fuzzy matching to no-NPI
         records and to within-state, to avoid merging different people.
    """
    # extract the last word of clean_name as the surname for phonetic coding
    last_name = F.element_at(F.split(F.col("clean_name"), " "), -1)

    return df.withColumn(
        "match_key",
        F.when(
            ~F.col("npi_missing"),
            F.concat(F.lit("npi:"), F.col("npi"))
        ).otherwise(
            # phonetic (sound-based) key: surname soundex + state
            F.concat(F.lit("fuzzy:"),
                     F.soundex(last_name),
                     F.lit("|"), F.col("clean_state"))
        )
    )


def resolve():
    spark = get_spark("gold-entity-resolution")

    silver_path = f"s3a://{BUCKET}/{SILVER_PREFIX}/providers"
    df = spark.read.parquet(silver_path)

    input_count = df.count()

    # 1. Assign the match key that decides which records are the same provider.
    df = build_match_key(df)

    # 2. For each provider group, pick the "best" value per field.
    #    Strategy: prefer the most complete / most recent record. We rank
    #    records within each match_key, preferring those WITH an NPI, then
    #    by processing time, and keep the top-ranked value for each field.
    rank_window = Window.partitionBy("match_key").orderBy(
        F.col("npi_missing").asc(),            # records with NPI first
        F.col("silver_processed_at").desc()    # then most recently processed
    )
    df_ranked = df.withColumn("rank", F.row_number().over(rank_window))

    # 3. The rank-1 record per group becomes the base of the golden record.
    golden_base = (df_ranked.filter(F.col("rank") == 1)
                   .select(
                       "match_key",
                       F.col("clean_name").alias("provider_name"),
                       F.col("npi").alias("npi"),
                       F.col("specialty").alias("specialty"),
                       F.col("clean_address").alias("address"),
                       F.col("clean_state").alias("state"),
                       F.col("clean_zip").alias("zip"),
                       F.col("license_expiry").alias("license_expiry"),
                   ))

    # 4. Add provenance: how many source records merged into each golden record,
    #    and which systems contributed. This is the 'data fabric' lineage story.
    provenance = (df.groupBy("match_key").agg(
        F.count("*").alias("source_record_count"),
        F.collect_set("source_system").alias("contributing_systems"),
    ))

    golden = golden_base.join(provenance, "match_key", "inner") \
        .withColumn("contributing_systems",
                    F.concat_ws(",", F.col("contributing_systems"))) \
        .withColumn("gold_processed_at", F.current_timestamp())

    golden_count = golden.count()
    duplicates_resolved = input_count - golden_count

    # 5. Report what entity resolution accomplished.
    print(f"Input (Silver) records:        {input_count}")
    print(f"Golden records (deduplicated): {golden_count}")
    print(f"Duplicate records merged away:  {duplicates_resolved}")
    print("\nExamples of merged providers (source_record_count > 1):")
    golden.filter(F.col("source_record_count") > 1) \
          .select("provider_name", "npi", "state",
                  "source_record_count", "contributing_systems") \
          .show(8, truncate=False)

    # 6. Write the golden records to the gold layer.
    out = f"s3a://{BUCKET}/{GOLD_PREFIX}/golden_providers"
    golden.write.mode("overwrite").parquet(out)
    print(f"Golden records written to: {out}")

    spark.stop()


if __name__ == "__main__":
    resolve()