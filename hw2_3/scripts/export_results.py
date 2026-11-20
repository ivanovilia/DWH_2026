"""Скачивание 6 S3 партиций parquet для сдачи ДЗ"""
import argparse
import os
from pathlib import Path

from upload_data import s3_client

DAYS = ("2026-01-23", "2026-01-24", "2026-01-25")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="demo", choices=("demo", "fixtures", "full"))
    parser.add_argument("--output-dir", default="/opt/airflow/exports")
    args = parser.parse_args()
    client = s3_client()
    bucket = os.environ.get("S3_BUCKET", "hw23")
    output = Path(args.output_dir)
    written = 0
    for table, partition in (("user_scd2", "snapshot_date"), ("campaign_recipients", "calculation_date")):
        for day in DAYS:
            prefix = f"results/{args.dataset}/{table}/{partition}={day}/"
            count = 0
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    relative_path = Path(key).relative_to(f"results/{args.dataset}")
                    destination = output / args.dataset / relative_path
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    client.download_file(bucket, key, str(destination))
                    count += 1
                    written += 1
            print(f"{prefix}: {count} files")
            if not count:
                raise RuntimeError(f"Partition absent: {prefix}")
    print(f"EXPORT COMPLETE: {written} objects in {output}/{args.dataset}")


if __name__ == "__main__":
    main()
