"""Print raw archive message totals and unique business ids; use before Spark backfill."""
import json
import os
from collections import Counter
import boto3
from botocore.config import Config


def main():
    ds = os.environ.get("FP_DATASET", "fixture")
    bucket = os.environ.get("S3_BUCKET", "cinema")
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                      config=Config(s3={"addressing_style": "path"}))
    for stream, id_col in [("events", "event_id"), ("profiles", "change_id")]:
        prefix = f"raw/{ds}/{stream}/"
        ids = set()
        files = 0
        messages = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".jsonl"):
                    continue
                files += 1
                body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
                for line in body.splitlines():
                    row = json.loads(line)
                    ids.add(row[id_col])
                    messages += 1
        print(f"{stream}: files={files} messages={messages} unique_ids={len(ids)}")


if __name__ == '__main__':
    main()
