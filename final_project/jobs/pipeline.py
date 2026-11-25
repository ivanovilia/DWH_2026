"""Пакетный конвейер обработки данных онлайн кинотеатра в Spark, время указано в UTC."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F, types as ST

from ch_client import query as ch_query

DATE_FMT = "%Y-%m-%d"
EVENT_SCHEMA = ST.StructType([
    ST.StructField("event_id", ST.StringType()),
    ST.StructField("event_time", ST.StringType()),
    ST.StructField("user_id", ST.LongType()),
    ST.StructField("event_type", ST.StringType()),
    ST.StructField("content_id", ST.LongType()),
    ST.StructField("watch_time_seconds", ST.LongType()),
    ST.StructField("query", ST.StringType()),
    ST.StructField("available_at", ST.StringType()),
])
PROFILE_SCHEMA = ST.StructType([
    ST.StructField("change_id", ST.StringType()),
    ST.StructField("user_id", ST.LongType()),
    ST.StructField("effective_at", ST.StringType()),
    ST.StructField("country", ST.StringType()),
    ST.StructField("subscription_tier", ST.StringType()),
    ST.StructField("available_at", ST.StringType()),
])
CATALOGUE_SCHEMA = ST.StructType([
    ST.StructField("content_id", ST.LongType()),
    ST.StructField("category", ST.StringType()),
    ST.StructField("duration_seconds", ST.LongType()),
])

MART_KEYS = {
    "minute": ["minute", "content_id"],
    "daily_country_tier": ["day", "country", "subscription_tier"],
    "top5": ["day", "rank"],
    "search_words": ["day", "word"],
}
MART_TABLES = {"minute": "batch_minute", "daily_country_tier": "batch_daily_country_tier",
               "top5": "batch_top5", "search_words": "batch_search_words"}
MART_COLUMNS = {
    "minute": ["day", "minute", "content_id", "views", "watched_seconds"],
    "daily_country_tier": ["day", "country", "subscription_tier", "views", "watched_seconds"],
    "top5": ["day", "rank", "content_id", "views", "watched_seconds"],
    "search_words": ["day", "word", "frequency"],
}
META = ["_dag_id", "_run_id", "_task_id", "_data_interval_start", "_data_interval_end", "_loaded_at"]


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def s3_client():
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                        config=Config(s3={"addressing_style": "path"}))


def bucket():
    return os.environ.get("S3_BUCKET", "cinema")


def s3prefix():
    return f"s3a://{bucket()}"


def spark_session(stage: str):
    endpoint = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
    jars = ",".join([
        "/opt/airflow/s3-jars/hadoop-aws-3.3.4.jar",
        "/opt/airflow/s3-jars/aws-java-sdk-bundle-1.12.262.jar",
    ])
    spark = (SparkSession.builder.appName(f"CinemaDWH-{stage}")
             .master(os.environ.get("SPARK_MASTER", "local[2]"))
             .config("spark.driver.memory", "1g")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.ui.enabled", "false")
             .config("spark.jars", jars)
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.endpoint", endpoint)
             .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https://")).lower())
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def assert_unique(frame: DataFrame, keys: list[str], name: str):
    duplicates = frame.groupBy(*keys).count().where(F.col("count") > 1)
    check(duplicates.limit(1).count() == 0, f"{name}: duplicate keys: {keys}")


def require_rows(frame: DataFrame, name: str):
    check(frame.limit(1).count() > 0, f"No rows in {name}. Is archive consumer caught up?")


def json_put(key: str, obj: Any):
    s3_client().put_object(Bucket=bucket(), Key=key,
                           Body=json.dumps(obj, ensure_ascii=False, indent=2).encode(),
                           ContentType="application/json")


def json_get(key: str):
    body = s3_client().get_object(Bucket=bucket(), Key=key)["Body"].read()
    return json.loads(body)


class Context:
    def __init__(self, args):
        self.day = date.fromisoformat(args.day)
        self.start = datetime.combine(self.day, datetime.min.time(), tzinfo=timezone.utc)
        self.end = self.start + timedelta(days=1)
        self.dataset = args.dataset
        self.run_id = args.run_id
        self.run_key = hashlib.sha256(args.run_id.encode()).hexdigest()[:20]
        self.dag_id = args.dag_id
        self.task_id = args.task_id
        self.loaded_at = datetime.now(timezone.utc)
        self.root = f"{s3prefix()}/work/{self.dataset}/{self.run_key}"
        self.manifest_root = f"work/{self.dataset}/{self.run_key}"

    def path(self, *pieces):
        return self.root + "/" + "/".join(pieces)

    def add_metadata(self, frame: DataFrame):
        for name, val in (("_dag_id", self.dag_id), ("_run_id", self.run_id),
                          ("_task_id", self.task_id)):
            frame = frame.withColumn(name, F.lit(val))
        for name, val in (("_data_interval_start", self.start),
                          ("_data_interval_end", self.end),
                          ("_loaded_at", self.loaded_at)):
            frame = frame.withColumn(name, F.lit(val).cast("timestamp"))
        return frame


def event_snapshot(spark, ctx):
    return spark.read.parquet(ctx.path("snapshot", "events"))


def profile_snapshot(spark, ctx):
    return spark.read.parquet(ctx.path("snapshot", "profiles"))


def read_catalogue(spark, ctx):
    return spark.read.parquet(ctx.path("snapshot", "catalogue"))


def scd2_history(spark, ctx):
    return spark.read.parquet(ctx.path("scd2"))


def snapshot_inputs(spark, ctx):
    base = f"{s3prefix()}/raw/{ctx.dataset}"
    events = (spark.read.schema(EVENT_SCHEMA).json(f"{base}/events/*/*.jsonl")
              .withColumn("event_time", F.to_timestamp("event_time"))
              .withColumn("available_at", F.to_timestamp("available_at")))
    changes = (spark.read.schema(PROFILE_SCHEMA).json(f"{base}/profiles/*/*.jsonl")
               .withColumn("effective_at", F.to_timestamp("effective_at"))
               .withColumn("available_at", F.to_timestamp("available_at")))
    catalogue = spark.read.schema(CATALOGUE_SCHEMA).json(f"{base}/catalogue/*.jsonl")
    require_rows(events, "archive events")
    require_rows(changes, "archive profile changes")
    require_rows(catalogue, "content catalogue")
    check(events.where("event_id IS NULL OR event_time IS NULL OR user_id IS NULL").limit(1).count() == 0,
          "Invalid event primary fields")
    check(changes.where("change_id IS NULL OR effective_at IS NULL OR country IS NULL OR subscription_tier IS NULL").limit(1).count() == 0,
          "Invalid profile change fields")
    assert_unique(catalogue, ["content_id"], "catalogue")
    check(catalogue.where("duration_seconds <= 0 OR duration_seconds IS NULL").limit(1).count() == 0,
          "Catalogue duration should be positive")
    # При повторной доставке идентификатор сохраняется. Выбираем запись с самым ранним временем поступления.
    event_w = Window.partitionBy("event_id").orderBy("available_at", "event_time")
    change_w = Window.partitionBy("change_id").orderBy("available_at", "effective_at")
    events = events.withColumn("_rn", F.row_number().over(event_w)).where("_rn=1").drop("_rn")
    changes = changes.withColumn("_rn", F.row_number().over(change_w)).where("_rn=1").drop("_rn")
    for name, frame in [("events", events), ("profiles", changes), ("catalogue", catalogue)]:
        frame.write.mode("overwrite").parquet(ctx.path("snapshot", name))
        print("SNAPSHOT", name, "rows", frame.count(), flush=True)
    json_put(f"{ctx.manifest_root}/snapshot.json", {
        "run_id": ctx.run_id, "day": str(ctx.day), "dataset": ctx.dataset,
        "captured_at": ctx.loaded_at.isoformat(),
        "note": "Inputs snapshotted from archived Kafka deliveries; wait for archive catch-up before scheduling",
    })


def build_scd2(spark, ctx):
    changes = profile_snapshot(spark, ctx)
    w = Window.partitionBy("user_id").orderBy("effective_at", "change_id")
    marked = (changes.withColumn("prev_country", F.lag("country").over(w))
              .withColumn("prev_tier", F.lag("subscription_tier").over(w)))
    versions = marked.where(
        F.col("prev_country").isNull() |
        (~F.col("country").eqNullSafe(F.col("prev_country"))) |
        (~F.col("subscription_tier").eqNullSafe(F.col("prev_tier")))
    ).select("user_id", "country", "subscription_tier", F.col("effective_at").alias("valid_from"))
    version_w = Window.partitionBy("user_id").orderBy("valid_from")
    versions = versions.withColumn("valid_to", F.lead("valid_from").over(version_w))
    assert_unique(versions, ["user_id", "valid_from"], "scd2")
    check(versions.where("valid_to IS NOT NULL AND valid_to <= valid_from").limit(1).count() == 0,
          "Non-positive SCD2 validity interval")
    versions = ctx.add_metadata(versions)
    versions.write.mode("overwrite").parquet(ctx.path("scd2"))
    print("SCD2 rows", versions.count(), "users", versions.select("user_id").distinct().count(), flush=True)


def daily_inputs(spark, ctx):
    events = event_snapshot(spark, ctx)
    views = events.where(
        (F.col("event_type") == "view") &
        (F.col("event_time") >= F.lit(ctx.start)) & (F.col("event_time") < F.lit(ctx.end))
    )
    searches = events.where(
        (F.col("event_type") == "search") &
        (F.col("event_time") >= F.lit(ctx.start)) & (F.col("event_time") < F.lit(ctx.end))
    )
    return views, searches


def joined_views(spark, ctx):
    views, searches = daily_inputs(spark, ctx)
    catalogue = read_catalogue(spark, ctx)
    scd2 = scd2_history(spark, ctx)
    joined = (views.alias("v")
              .join(catalogue.alias("c"), F.col("v.content_id") == F.col("c.content_id"), "inner")
              .join(scd2.alias("p"), (F.col("v.user_id") == F.col("p.user_id")) &
                    (F.col("p.valid_from") <= F.col("v.event_time")) &
                    (F.col("p.valid_to").isNull() | (F.col("v.event_time") < F.col("p.valid_to"))), "inner")
              .select(F.col("v.event_id"), F.col("v.event_time"), F.col("v.user_id"),
                      F.col("v.content_id"), F.col("v.watch_time_seconds"),
                      F.col("c.category"), F.col("p.country"), F.col("p.subscription_tier")))
    return joined, views, searches


def words_df(searches: DataFrame):
    return (searches.select(F.to_date("event_time").alias("day"),
                            F.explode(F.split(F.lower(F.trim(F.col("query"))), r"\s+")).alias("word"))
            .where(F.length("word") > 4)
            .groupBy("day", "word").agg(F.count("*").alias("frequency")))


def calculate_marts(spark, ctx):
    joined, views, searches = joined_views(spark, ctx)
    print("HISTORICAL JOIN PHYSICAL PLAN", flush=True)
    joined.explain(mode="formatted")
    # Проверяем, что ошибки в ключах каталога или истории SCD2 не привели к дублированию строк при соединении.
    assert_unique(joined, ["event_id"], "joined views")
    check(joined.count() == views.count(), "Not every view matched exactly one catalogue and profile")
    check(joined.where("watch_time_seconds IS NULL OR watch_time_seconds < 0").limit(1).count() == 0,
          "Invalid watched duration")
    minute = (joined.groupBy(F.to_date("event_time").alias("day"),
                             F.date_trunc("minute", F.col("event_time")).alias("minute"), "content_id")
              .agg(F.count("*").alias("views"), F.sum("watch_time_seconds").alias("watched_seconds")))
    daily = (joined.groupBy(F.to_date("event_time").alias("day"), "country", "subscription_tier")
             .agg(F.count("*").alias("views"), F.sum("watch_time_seconds").alias("watched_seconds")))
    top_counts = joined.groupBy(F.to_date("event_time").alias("day"), "content_id").agg(
        F.count("*").alias("views"), F.sum("watch_time_seconds").alias("watched_seconds"))
    top_w = Window.partitionBy("day").orderBy(F.desc("views"), F.asc("content_id"))
    top = (top_counts.withColumn("rank", F.row_number().over(top_w)).where("rank <= 5")
           .select("day", "rank", "content_id", "views", "watched_seconds"))
    words = words_df(searches)
    for name, frame in [("minute", minute), ("daily_country_tier", daily),
                        ("top5", top), ("search_words", words)]:
        ctx.add_metadata(frame).write.mode("overwrite").parquet(ctx.path("marts", name))
        print("MART", name, "rows", frame.count(), flush=True)


def assert_multiset_equal(actual, expected, name):
    left = actual.exceptAll(expected).limit(1).count()
    right = expected.exceptAll(actual).limit(1).count()
    check(left == 0 and right == 0, f"{name}: data mismatch left={left},right={right}")


def validate_rdd(spark, ctx, joined: DataFrame, searches: DataFrame, top: DataFrame, words: DataFrame):
    """Независимо рассчитывает показатели через RDD без использования агрегаций DataFrame."""
    # Учитываем каждое вхождение слова, даже если оно повторяется в одном поисковом запросе.
    rdd_word = (searches.select("event_time", "query").rdd
                .flatMap(lambda row: [((row[0].date().isoformat(), token), 1)
                                      for token in (row[1] or "").lower().split() if len(token) > 4])
                .reduceByKey(lambda a, b: a + b)
                .map(lambda x: (x[0][0], x[0][1], x[1])))
    rdd_word_rows = sorted(rdd_word.collect())
    df_word_rows = sorted((str(r.day), r.word, r.frequency) for r in words.collect())
    check(rdd_word_rows == df_word_rows, "RDD/DF search-word frequencies differ")

    # Для дневного топ-5 отдельно считаем просмотры каждого объекта по строкам, полученным после соединения.
    rdd_top_counts = (joined.select("event_time", "content_id", "watch_time_seconds").rdd
                      .map(lambda row: ((row[0].date().isoformat(), row[1]), (1, row[2])))
                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))
    rdd_top = (rdd_top_counts.map(lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1])))
               .groupByKey()
               .flatMap(lambda x: [(x[0], rank, content, count, duration)
                                   for rank, (content, count, duration) in enumerate(
                                       sorted(x[1], key=lambda r: (-r[1], r[0]))[:5], 1)]))
    rdd_top_rows = sorted(rdd_top.collect())
    df_top_rows = sorted((str(r.day), r.rank, r.content_id, r.views, r.watched_seconds) for r in top.collect())
    check(rdd_top_rows == df_top_rows, "RDD/DF daily top-5 differ")
    return {"rdd_df_word_equal": True, "rdd_df_top5_equal": True,
            "rdd_words": len(rdd_word_rows), "rdd_top5": len(rdd_top_rows)}


def validate_marts(spark, ctx):
    scd2 = scd2_history(spark, ctx)
    assert_unique(scd2, ["user_id", "valid_from"], "SCD2")
    w = Window.partitionBy("user_id").orderBy("valid_from")
    history_checked = scd2.withColumn("next_from", F.lead("valid_from").over(w))
    check(history_checked.where("(valid_to IS NULL AND next_from IS NOT NULL) OR "
                                "(valid_to IS NOT NULL AND (next_from IS NULL OR valid_to != next_from))")
          .limit(1).count() == 0, "SCD2 has gaps, overlaps, or multiple open versions")
    check(scd2.where("valid_to IS NULL").groupBy("user_id").count()
          .where("count != 1").limit(1).count() == 0, "Current SCD2 version count incorrect")
    joined, views, searches = joined_views(spark, ctx)
    assert_unique(joined, ["event_id"], "joined view")
    check(joined.count() == views.count(), "Views have missing/ambiguous catalogue/profile matches")
    marts = {name: spark.read.parquet(ctx.path("marts", name)) for name in MART_KEYS}
    for name, frame in marts.items():
        assert_unique(frame, MART_KEYS[name], name)
        check(frame.where(F.col("_run_id") != ctx.run_id).limit(1).count() == 0,
              f"{name}: wrong run metadata")
    check(marts["top5"].where("rank < 1 OR rank > 5").limit(1).count() == 0, "Invalid top-5 rank")
    sum_minute = marts["minute"].agg(F.sum("views"), F.sum("watched_seconds")).first()
    sum_daily = marts["daily_country_tier"].agg(F.sum("views"), F.sum("watched_seconds")).first()
    expected = joined.agg(F.count("*"), F.sum("watch_time_seconds")).first()
    # Для дня без данных нулевой результат допустим.
    numbers = tuple(int(x or 0) for x in sum_minute)
    check(numbers == tuple(int(x or 0) for x in expected), "Minute totals do not match unique views")
    check(tuple(int(x or 0) for x in sum_daily) == numbers, "Country/tier totals differ from minute")
    rdd = validate_rdd(spark, ctx, joined, searches, marts["top5"], marts["search_words"])
    result = {
        "dataset": ctx.dataset, "day": str(ctx.day), "run_id": ctx.run_id,
        "unique_views": int(expected[0]), "watched_seconds": int(expected[1] or 0),
        "marts": {name: int(frame.count()) for name, frame in marts.items()},
        "scd2_rows": int(scd2.count()), "status": "passed", **rdd,
    }
    json_put(f"{ctx.manifest_root}/validation.json", result)
    print("VALIDATION PASSED", json.dumps(result, ensure_ascii=False), flush=True)


def python_value(value, name=None):
    if isinstance(value, datetime):
        if name == "_loaded_at":
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    return value


def upload_clickhouse_rows(table: str, frame: DataFrame, columns: list[str], publish_id: str):
    count = 0
    chunk = []
    for row in frame.select(*columns).toLocalIterator():
        item = {name: python_value(row[name], name) for name in columns}
        item["publish_id"] = publish_id
        chunk.append(json.dumps(item, ensure_ascii=False))
        if len(chunk) >= 500:
            ch_query(f"INSERT INTO {table} FORMAT JSONEachRow", "\n".join(chunk))
            count += len(chunk)
            chunk = []
    if chunk:
        ch_query(f"INSERT INTO {table} FORMAT JSONEachRow", "\n".join(chunk))
        count += len(chunk)
    return count


def publish_marts(spark, ctx):
    validation = json_get(f"{ctx.manifest_root}/validation.json")
    check(validation.get("status") == "passed" and validation.get("run_id") == ctx.run_id,
          "Publication requires successful validation for the same run")
    publish_id = uuid.uuid4().hex
    # Старые версии сохраняются. При чтении учитывается только версия, на которую указывает запись за день.
    for name, table in MART_TABLES.items():
        frame = spark.read.parquet(ctx.path("marts", name))
        columns = MART_COLUMNS[name] + META
        n = upload_clickhouse_rows(table, frame, columns, publish_id)
        db_n = int(ch_query(f"SELECT count() FROM {table} FINAL WHERE publish_id='{publish_id}'"))
        check(db_n == n == validation["marts"][name],
              f"ClickHouse {name}: expected {validation['marts'][name]} inserted={n} stored={db_n}")
        print(f"PUBLISHED {table} rows={n} publish_id={publish_id}", flush=True)
    revision = time.time_ns()
    # Эта последняя вставка одновременно публикует данные всех четырёх витрин.
    ch_query(f"INSERT INTO published_days (day,publish_id,_run_id,revision,published_at) VALUES "
             f"('{ctx.day}','{publish_id}','{ctx.run_id}',{revision},now64(3))")
    # Сдвигаем границу T от текущего значения только через полностью опубликованные дни, идущие подряд.
    current_iso = ch_query("SELECT formatDateTime(t,'%Y-%m-%d') FROM report_cutover FINAL WHERE id=1").strip()
    current = date.fromisoformat(current_iso)
    published = {line.strip() for line in ch_query("SELECT toString(day) FROM published_days FINAL").splitlines()}
    candidate = current
    while candidate.isoformat() in published:
        candidate += timedelta(days=1)
    if candidate > current:
        t = candidate.strftime("%Y-%m-%d 00:00:00")
        ch_query(f"INSERT INTO report_cutover (id,t,revision) VALUES "
                 f"(1,toDateTime('{t}','UTC'),{revision + 1})")
    print(f"PUBLISH OK day={ctx.day} publish_id={publish_id} cutover={candidate}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["snapshot", "scd2", "marts", "validate", "publish"])
    parser.add_argument("--day", required=True, help="UTC date YYYY-MM-DD")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--dataset", default=os.getenv("FP_DATASET", "fixture"))
    args = parser.parse_args()
    ctx = Context(args)
    spark = spark_session(args.stage) if args.stage != "publish" else spark_session("publish")
    try:
        getattr(sys.modules[__name__], {
            "snapshot": "snapshot_inputs", "scd2": "build_scd2",
            "marts": "calculate_marts", "validate": "validate_marts",
            "publish": "publish_marts",
        }[args.stage])(spark, ctx)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
