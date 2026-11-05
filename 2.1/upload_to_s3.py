from __future__ import annotations

import os
import time
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

BUCKET = "dwh-homework"
PREFIX = "raw/user_events/"
SOURCE_DIR = Path(__file__).parent / "event_log"


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL", "http://rustfs:9000"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def main():
    files = sorted(SOURCE_DIR.glob("events_*.csv"))
    if not files:
        raise RuntimeError(f"Нет CSV в {SOURCE_DIR}.")

    s3 = s3_client()
    for attempt in range(30):
        try:
            s3.list_buckets()
            break
        except (BotoCoreError, ClientError, EndpointConnectionError):
            if attempt == 29:
                raise
            time.sleep(2)

    if BUCKET not in [x["Name"] for x in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=BUCKET)

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        for i in range(0, len(objects), 1000):
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objects[i:i+1000]})

    for file in files:
        key = PREFIX + file.name
        s3.upload_file(str(file), BUCKET, key)
        print(f"s3://{BUCKET}/{key}")
    print(f"Готово: {len(files)} CSV-файлов загружено в S3.")


if __name__ == "__main__":
    main()
