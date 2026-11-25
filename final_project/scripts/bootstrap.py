"""Создаёт бакет S3, каталог и схемы ClickHouse"""
from __future__ import annotations
import os
import sys
import time
from pathlib import Path
import boto3
from botocore.config import Config
from ch_client import query


def main():
    bucket = os.environ.get("S3_BUCKET", "cinema")
    ds = os.environ.get("FP_DATASET", "fixture")
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                      config=Config(s3={"addressing_style": "path"}))
    if bucket not in [row["Name"] for row in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=bucket)
    source = Path(f"/opt/airflow/data/{ds}/catalogue.jsonl")
    s3.put_object(Bucket=bucket, Key=f"raw/{ds}/catalogue/catalogue.jsonl",
                  Body=source.read_bytes())
    sql_path = Path("/opt/airflow/sql/schema.sql")
    content = sql_path.read_text(encoding="utf-8")
    # SQL-команды разделяет строка, содержащая только маркер '-- SPLIT --'.
    for statement in content.split("\n-- SPLIT --\n"):
        statement = statement.strip()
        if statement:
            query(statement)
    # При повторной инициализации сохраняем текущую границу между пакетными и оперативными данными отчёта.
    if int(query("SELECT count() FROM report_cutover")) == 0:
        query("INSERT INTO report_cutover (id,t,revision) VALUES (1, toDateTime('2026-01-01 00:00:00','UTC'),1)")
    print(f"BOOTSTRAP OK dataset={ds}, bucket={bucket}, schemas and Kafka MV initialized")


if __name__ == "__main__":
    main()
