# pipeline/bronze_ingest.py
# Bronze layer: upload the raw CSV files into MinIO exactly as-is.
# No cleaning happens here — Bronze preserves pristine source data.

import boto3
import os
import sys

# Make the 'config' folder importable no matter where we run from
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, BUCKET, BRONZE_PREFIX
)


def get_s3_client():
    """Creates a connection to MinIO using the S3 protocol.
    The same code works against real AWS S3 by changing the endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def upload_raw_files():
    """Uploads each CSV from local raw_data/ into the bronze area of the bucket."""
    s3 = get_s3_client()

    files_to_upload = [
        "raw_data/system_a_providers.csv",
        "raw_data/system_b_providers.csv",
    ]

    for local_path in files_to_upload:
        if not os.path.exists(local_path):
            print(f"WARNING: {local_path} not found — did you run the generator?")
            continue

        filename = os.path.basename(local_path)
        # the 'key' is the full path inside the bucket, e.g. bronze/system_a_providers.csv
        key = f"{BRONZE_PREFIX}/{filename}"

        s3.upload_file(local_path, BUCKET, key)
        print(f"Uploaded {local_path}  ->  s3://{BUCKET}/{key}")

    print("\nBronze ingestion complete.")


if __name__ == "__main__":
    upload_raw_files()