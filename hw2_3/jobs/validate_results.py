"""Проверка результатов трёх запусков; сравнение до/после Clear задач"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, Window, functions as F

from build_campaign_recipients import calculate_recipients
from hw23_common import (
    ATTRS, META, RECIPIENT_COLS, SCD2_COLS, RunInfo, read_campaigns,
    read_events, recipients_path, scd2_path, spark_session,
)

DAYS = ("2026-01-23", "2026-01-24", "2026-01-25")
EXPECTED_DEMO = {
    "2026-01-23": (961, 100, 53),
    "2026-01-24": (995, 100, 25),
    "2026-01-25": (1036, 100, 22),
}


def assert_zero(label: str, number: int) -> None:
    print(f"  {label}: {number}", flush=True)
    if number:
        raise AssertionError(f"{label}: {number} (ожидалось 0)")


def compare_exact(left: DataFrame, right: DataFrame, cols: tuple[str, ...], label: str) -> None:
    """Полное сравнение множества строк с учётом повторов в обе стороны."""
    a, b = left.select(*cols), right.select(*cols)
    missing = a.exceptAll(b).count()
    extra = b.exceptAll(a).count()
    print(f"  COMPARE {label}: missing={missing}, extra={extra}", flush=True)
    if missing or extra:
        raise AssertionError(f"Несовпадение таблиц {label}: {missing} / {extra}")


def metadata_checks(df: DataFrame, day: str, expected_task: str, label: str) -> dict:
    print(f"  META {label}", flush=True)
    missing_expr = [F.sum(F.col(x).isNull().cast("long")).alias(x) for x in META]
    missing = df.agg(*missing_expr).first().asDict()
    for name, n in missing.items():
        assert_zero(f"NULL {name}", n or 0)
    rows = df.groupBy(*META).count().collect()
    if not rows:
        print("    EMPTY: в Parquet нет строк; метаполя проверяются по логу Airflow", flush=True)
        return {}
    if len(rows) != 1:
        raise AssertionError(f"{label}: метаполя не одинаковы на всех строках: {len(rows)} вариантов")
    data = rows[0].asDict()
    start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    if data["_task_id"] != expected_task:
        raise AssertionError(f"Неожиданный task_id {data['_task_id']}")
    if data["_dag_id"] != "hw23_daily_scd2_campaigns":
        raise AssertionError(f"Неожиданный dag_id {data['_dag_id']}")
    if not data["_run_id"].startswith("scheduled__"):
        raise AssertionError(f"Требовался scheduled run, получен: {data['_run_id']}")
    # В Spark с TZ=UTC timestamp считывается как naive datetime UTC.
    if data["_data_interval_start"] != start.replace(tzinfo=None):
        raise AssertionError(f"Начало интервала отличается: {data['_data_interval_start']}")
    if data["_data_interval_end"] != end.replace(tzinfo=None):
        raise AssertionError(f"Конец интервала отличается: {data['_data_interval_end']}")
    print(
        "    " + ", ".join(f"{name}={data[name]}" for name in META) + f", rows={data['count']}",
        flush=True,
    )
    # Запрошенная в задании сводка: число строк + min/max(_loaded_at).
    summary = (
        df.groupBy(*(x for x in META if x != "_loaded_at"))
        .agg(
            F.count("*").alias("rows"),
            F.min("_loaded_at").alias("min_loaded_at"),
            F.max("_loaded_at").alias("max_loaded_at"),
        )
        .first()
    )
    print(
        f"    META SUMMARY rows={summary['rows']} "
        f"min_loaded_at={summary['min_loaded_at']} "
        f"max_loaded_at={summary['max_loaded_at']}",
        flush=True,
    )
    return data


def check_scd2(frame: DataFrame, day: str, all_events: DataFrame) -> tuple[int, int]:
    print(f"\nSCD2 {day}", flush=True)
    if set(frame.columns) != set(SCD2_COLS):
        raise AssertionError(f"Схема SCD2 не совпадает: {frame.columns}")
    n = frame.count()
    users = frame.select("user_id").distinct().count()
    print(f"  rows={n}, users={users}", flush=True)
    for name in SCD2_COLS:
        if name != "valid_to":
            assert_zero(f"NULL {name}", frame.where(F.col(name).isNull()).count())
    assert_zero("wrong snapshot_date", frame.where(F.col("snapshot_date") != F.lit(day).cast("date")).count())
    assert_zero("duplicate user_id/valid_from", frame.groupBy("user_id", "valid_from").count().where("count > 1").count())
    assert_zero("invalid active flag", frame.where(~F.col("is_current").eqNullSafe(F.col("valid_to").isNull())).count())
    assert_zero("nonpositive validity interval", frame.where(F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from"))).count())
    assert_zero("wrong current version count", frame.groupBy("user_id").agg(F.sum(F.col("is_current").cast("int")).alias("cnt")).where("cnt != 1").count())
    order = Window.partitionBy("user_id").orderBy("valid_from")
    seq = frame.withColumn("_next_start", F.lead("valid_from").over(order))
    assert_zero("gaps / overlaps", seq.where(~F.col("valid_to").eqNullSafe(F.col("_next_start"))).count())
    for attr in ATTRS:
        seq = seq.withColumn("_prev_" + attr, F.lag(attr).over(order))
    seq = seq.withColumn("_rn", F.row_number().over(order))
    same = F.lit(True)
    for attr in ATTRS:
        same = same & F.col(attr).eqNullSafe(F.col("_prev_" + attr))
    assert_zero("unnecessary versions (identical adjacent profiles)", seq.where((F.col("_rn") > 1) & same).count())
    end = datetime.fromisoformat(day).replace(tzinfo=timezone.utc) + timedelta(days=1)
    assert_zero("future versions", frame.where(F.col("valid_from") >= F.lit(end)).count())
    expected_users = all_events.where(F.col("event_time") < F.lit(end)).select("user_id").distinct().count()
    if users != expected_users:
        raise AssertionError(f"user count mismatch {users} != {expected_users}")
    return n, users


def check_recipients(frame: DataFrame, day: str, scd2: DataFrame, campaigns: DataFrame) -> int:
    print(f"\nRECIPIENTS {day}", flush=True)
    if set(frame.columns) != set(RECIPIENT_COLS):
        raise AssertionError(f"Схема получателей не совпадает: {frame.columns}")
    n = frame.count()
    print(f"  rows={n}", flush=True)
    for name in RECIPIENT_COLS:
        if name != "user_valid_to":
            assert_zero(f"NULL {name}", frame.where(F.col(name).isNull()).count())
    assert_zero("wrong calculation_date", frame.where(F.col("calculation_date") != F.lit(day).cast("date")).count())
    assert_zero("duplicate campaign_id/user_id", frame.groupBy("campaign_id", "user_id").count().where("count > 1").count())
    assert_zero(
        "domain mismatch",
        frame.where(F.lower(F.substring_index("user_email", "@", -1)) != F.lower(F.col("email_domain"))).count(),
    )
    assert_zero(
        "historical interval mismatch",
        frame.where(~((F.col("user_valid_from") <= F.col("send_at")) &
                      (F.col("user_valid_to").isNull() | (F.col("send_at") < F.col("user_valid_to"))))).count(),
    )
    start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    assert_zero("campaign outside processing day", frame.where(~((F.col("send_at") >= F.lit(start)) & (F.col("send_at") < F.lit(end)))).count())
    run = RunInfo("demo", "check", "check", "check", start, end, datetime.now(timezone.utc))
    expected = calculate_recipients(scd2, campaigns, run)
    cols = tuple(x for x in RECIPIENT_COLS if x not in META)
    compare_exact(expected, frame, cols, f"recompute JOIN {day}")
    selected = campaigns.where((F.col("send_at") >= F.lit(start)) & (F.col("send_at") < F.lit(end)))
    counts = frame.groupBy("campaign_id").count()
    for r in selected.select("campaign_id", "campaign_name").join(counts, "campaign_id", "left").fillna(0, ["count"]).orderBy("campaign_id").collect():
        print(f"  campaign {r.campaign_id} ({r.campaign_name}): recipients={r['count']}", flush=True)
    return n


def baseline_path(dataset: str, table: str, day: str) -> str:
    key = "snapshot_date" if table == "user_scd2" else "calculation_date"
    bucket = os.environ.get("S3_BUCKET", "hw23")
    return f"s3a://{bucket}/_verification/hw23/{dataset}/{table}/{key}={day}/"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True, choices=("before", "after"))
    parser.add_argument("--dataset", default="demo", choices=("demo", "fixtures", "full"))
    args = parser.parse_args()
    spark = spark_session(f"HW23 verification {args.phase}")
    try:
        campaigns = read_campaigns(spark, args.dataset).cache()
        events = read_events(spark, args.dataset).cache()
        first_metadata = {}
        for day in DAYS:
            s = spark.read.parquet(scd2_path(args.dataset, day))
            r = spark.read.parquet(recipients_path(args.dataset, day))
            n_versions, n_users = check_scd2(s, day, events)
            n_recipients = check_recipients(r, day, s, campaigns)
            a = metadata_checks(s, day, "build_user_scd2", "SCD2")
            b = metadata_checks(r, day, "build_campaign_recipients", "recipients")
            if a and b and a["_run_id"] != b["_run_id"]:
                raise AssertionError(f"Run IDs differ for {day}")
            if a and b and a["_data_interval_start"] != b["_data_interval_start"]:
                raise AssertionError(f"Interval starts differ for {day}")
            if a and b and a["_data_interval_end"] != b["_data_interval_end"]:
                raise AssertionError(f"Interval ends differ for {day}")
            if args.dataset == "demo":
                if (n_versions, n_users, n_recipients) != EXPECTED_DEMO[day]:
                    raise AssertionError(f"Unexpected demo counts for {day}: {(n_versions, n_users, n_recipients)}")
            if day == DAYS[0]:
                first_metadata = {"SCD2": a, "recipients": b}

            for table, data in (("user_scd2", s), ("campaign_recipients", r)):
                base = baseline_path(args.dataset, table, day)
                columns = SCD2_COLS if table == "user_scd2" else RECIPIENT_COLS
                if args.phase == "before":
                    data.select(*columns).write.mode("overwrite").parquet(base)
                    print(f"  BASELINE SAVED: {base}", flush=True)
                else:
                    original = spark.read.parquet(base)
                    checked = tuple(x for x in columns if x not in META) if day == DAYS[0] else columns
                    compare_exact(original, data, checked, f"{table}/{day}")
                    if day == DAYS[0] and first_metadata.get(table):
                        old = original.select("_run_id").distinct().collect()
                        new = data.select("_run_id").distinct().collect()
                        if old and new and old[0][0] != new[0][0]:
                            raise AssertionError(f"run_id changed after Clear for {table}/{day}")
        print(f"\nALL CHECKS PASSED: phase={args.phase}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
