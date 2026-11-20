"""Общие функции расчётов ДЗ - Spark 3.5.4, RustFS"""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DateType, LongType, StringType, StructField, StructType, TimestampType,
)

ATTRS = ("user_fio", "user_email", "user_login")
META = (
    "_dag_id", "_run_id", "_task_id", "_data_interval_start",
    "_data_interval_end", "_loaded_at",
)
SCD2_COLS = (
    "user_id", "user_fio", "user_email", "user_login", "valid_from",
    "valid_to", "is_current", "snapshot_date", *META,
)
RECIPIENT_COLS = (
    "campaign_id", "campaign_name", "send_at", "email_domain", "user_id",
    "user_fio", "user_email", "user_login", "user_valid_from",
    "user_valid_to", "calculation_date", *META,
)
EVENT_SCHEMA = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_fio", StringType(), True),
    StructField("user_email", StringType(), True),
    StructField("user_login", StringType(), True),
])
CAMPAIGN_SCHEMA = StructType([
    StructField("campaign_id", LongType(), True),
    StructField("email_domain", StringType(), True),
    StructField("send_at", TimestampType(), True),
    StructField("campaign_name", StringType(), True),
])


def spark_session(app_name: str) -> SparkSession:
    endpoint = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
    jars_root = "/opt/airflow/s3-jars/"
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.jars",
            jars_root + "hadoop-aws-3.3.4.jar," + jars_root + "aws-java-sdk-bundle-1.12.262.jar",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def s3_root(dataset: str) -> str:
    return f"s3a://{os.environ.get('S3_BUCKET', 'hw23')}/results/{dataset}"


def raw_path(dataset: str, name: str) -> str:
    return f"s3a://{os.environ.get('S3_BUCKET', 'hw23')}/raw/{dataset}/{name}"


def scd2_path(dataset: str, day: str) -> str:
    return f"{s3_root(dataset)}/user_scd2/snapshot_date={day}/"


def recipients_path(dataset: str, day: str) -> str:
    return f"{s3_root(dataset)}/campaign_recipients/calculation_date={day}/"


def read_events(spark: SparkSession, dataset: str) -> DataFrame:
    return (
        spark.read.schema(EVENT_SCHEMA)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX")
        .csv(raw_path(dataset, "events/*.csv"))
    )


def read_campaigns(spark: SparkSession, dataset: str) -> DataFrame:
    return (
        spark.read.schema(CAMPAIGN_SCHEMA)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX")
        .csv(raw_path(dataset, "campaigns.csv"))
    )


def parse_utc(text: str) -> datetime:
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError(f"Timezone обязателен (UTC): {text}")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class RunInfo:
    dataset: str
    dag_id: str
    run_id: str
    task_id: str
    start: datetime
    end: datetime
    loaded_at: datetime

    @property
    def day(self) -> str:
        return self.start.date().isoformat()

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "RunInfo":
        start = parse_utc(args.start)
        end = parse_utc(args.end)
        if end - start != timedelta(days=1) or start.hour != 0 or start.minute != 0 or start.second != 0:
            raise ValueError(f"Ожидались одни сутки UTC: [{start}, {end})")
        if not args.dag_id or not args.run_id or not args.task_id:
            raise ValueError("Метаполя DAG, run и task не могут быть пустыми")
        return cls(
            dataset=args.dataset,
            dag_id=args.dag_id,
            run_id=args.run_id,
            task_id=args.task_id,
            start=start,
            end=end,
            loaded_at=datetime.now(timezone.utc),  # Фиксируем ровно один раз на попытку
        )


def add_run_metadata(frame: DataFrame, run: RunInfo) -> DataFrame:
    # Времена передаём ISO-строками и приводим в Spark, настроенном на UTC.
    for name, value in (("_dag_id", run.dag_id), ("_run_id", run.run_id), ("_task_id", run.task_id)):
        frame = frame.withColumn(name, F.lit(value).cast("string"))
    for name, value in (
        ("_data_interval_start", run.start),
        ("_data_interval_end", run.end),
        ("_loaded_at", run.loaded_at),
    ):
        frame = frame.withColumn(name, F.to_timestamp(F.lit(value.isoformat())))
    return frame


def add_cli_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dataset", default="demo", choices=("demo", "fixtures", "full"))
    for name in ("dag-id", "run-id", "task-id", "start", "end"):
        parser.add_argument("--" + name, required=True)
