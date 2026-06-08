# pipeline/spark_session.py
# Builds a Spark session configured to read/write MinIO (S3-compatible) storage.
# Every pipeline script imports get_spark() from here, so the connection
# setup lives in exactly one place.

import os
import sys
from pyspark.sql import SparkSession

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY


def get_spark(app_name="provider-trust"):
    """Creates a Spark session wired to talk to MinIO over the S3 protocol.

    The two .config 'packages' lines pull in the connector libraries Spark
    needs to speak S3 — Spark downloads them automatically on first run.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Connector libraries that let Spark read/write s3a:// paths.
        # These versions are matched to Spark 3.5.x — do not change casually.
        # send Ivy's download cache to /tmp, which the container user can write to
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        # --- point Spark's S3 client at MinIO ---
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        # MinIO needs "path style" access (bucket in the path, not the hostname)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # quieten Spark's noisy logs
    return spark