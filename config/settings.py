# config/settings.py
# Central configuration shared by all pipeline scripts.
# To move from local MinIO to real AWS S3, you'd change only S3_ENDPOINT
# and the credentials — the rest of the code stays identical.

# Connection details for our local MinIO (the S3 stand-in)
import os

# Credentials are read from environment variables (set via docker-compose / .env),
# never hardcoded. The fallback defaults are local-only MinIO demo values.
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "minioadmin123")

# The bucket and the "folders" (prefixes) for each medallion layer
BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"