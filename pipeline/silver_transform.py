# pipeline/silver_transform.py
# Silver layer: read both raw source files, standardize the messy fields,
# unify them into one table, and write the cleaned result back to MinIO.
# This does NOT merge duplicates yet — it only makes records clean & comparable.

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pipeline.spark_session import get_spark
from config.settings import BUCKET, BRONZE_PREFIX, SILVER_PREFIX


def standardize_name(col):
    """Turns messy provider names into one clean, comparable form.
    Removes titles (Dr.), credential suffixes (MD), punctuation, extra spaces,
    and lowercases — so 'Dr. Robert Smith, MD' and 'robert smith' match."""
    cleaned = F.lower(col)
    # remove common titles and credential suffixes
    cleaned = F.regexp_replace(cleaned, r"\b(dr|mr|mrs|ms)\.?\b", "")
    cleaned = F.regexp_replace(cleaned, r",?\s*(md|do|np|pa|rn)\b", "")
    # remove anything that isn't a letter or space
    cleaned = F.regexp_replace(cleaned, r"[^a-z ]", "")
    # collapse multiple spaces into one, then trim ends
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    return F.trim(cleaned)


def standardize_address(col):
    """Uppercases and standardizes common street-type abbreviations so
    '123 Main Street' and '123 MAIN ST' become identical."""
    a = F.upper(col)
    a = F.regexp_replace(a, r"\bSTREET\b", "ST")
    a = F.regexp_replace(a, r"\bAVENUE\b", "AVE")
    a = F.regexp_replace(a, r"\bROAD\b", "RD")
    a = F.regexp_replace(a, r"\bDRIVE\b", "DR")
    a = F.regexp_replace(a, r"\s+", " ")
    return F.trim(a)


def transform():
    spark = get_spark("silver-transform")

    bronze_base = f"s3a://{BUCKET}/{BRONZE_PREFIX}"

    # 1. Read both source files. add_source tags each row with its origin.
    df_a = (spark.read.option("header", True)
            .csv(f"{bronze_base}/system_a_providers.csv")
            .withColumn("source_system", F.lit("system_a")))
    df_b = (spark.read.option("header", True)
            .csv(f"{bronze_base}/system_b_providers.csv")
            .withColumn("source_system", F.lit("system_b")))

    # 2. Stack the two into one unified DataFrame.
    #    unionByName matches columns by name (safer than by position).
    df = df_a.unionByName(df_b)

    # 3. Apply standardization, creating new clean_* columns.
    #    We KEEP the originals too — never destroy source values in Silver.
    df = (df
          .withColumn("clean_name", standardize_name(F.col("provider_name")))
          .withColumn("clean_address", standardize_address(F.col("address")))
          .withColumn("clean_state", F.upper(F.trim(F.col("state"))))
          .withColumn("clean_zip", F.trim(F.col("zip")))
          # flag rows missing an NPI rather than dropping them
          .withColumn("npi_missing",
                      F.when((F.col("npi").isNull()) | (F.trim(F.col("npi")) == ""),
                             F.lit(True)).otherwise(F.lit(False)))
          # processing timestamp = basic lineage/audit
          .withColumn("silver_processed_at", F.current_timestamp()))

    # 4. Quick visibility into what we produced
    total = df.count()
    missing_npi = df.filter(F.col("npi_missing")).count()
    print(f"Silver records: {total}")
    print(f"Records missing NPI (flagged, not dropped): {missing_npi}")
    print("Sample of standardized data:")
    df.select("source_system", "provider_name", "clean_name",
              "address", "clean_address", "npi_missing").show(8, truncate=False)

    # 5. Write to the silver area as Parquet (columnar format, far more
    #    efficient than CSV — the standard for analytics layers).
    out = f"s3a://{BUCKET}/{SILVER_PREFIX}/providers"
    df.write.mode("overwrite").parquet(out)
    print(f"\nSilver written to: {out}")

    spark.stop()


if __name__ == "__main__":
    transform()