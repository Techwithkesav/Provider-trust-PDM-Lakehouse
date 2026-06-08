# config/settings.py
# Central configuration shared by all pipeline scripts.
# To move from local MinIO to real AWS S3, you'd change only S3_ENDPOINT
# and the credentials — the rest of the code stays identical.

# Connection details for our local MinIO (the S3 stand-in)
S3_ENDPOINT = "http://minio:9000"   # MinIO's storage API port
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin123"

# The bucket and the "folders" (prefixes) for each medallion layer
BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"