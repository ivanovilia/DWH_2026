"""Задача Airflow 1- независимый ежедневный снимок SCD2 из всех событий до конца суток"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, Window, functions as F

from hw23_common import (
    ATTRS, SCD2_COLS, RunInfo, add_cli_args, add_run_metadata, read_events,
    scd2_path, spark_session,
)


def calculate_scd2(events: DataFrame, end_utc, run: RunInfo) -> DataFrame:
    # Историю до начала суток нельзя отбрасывать
    previous_events = events.where(F.col("event_time") < F.lit(end_utc))
    order = Window.partitionBy("user_id").orderBy(F.col("event_time").asc())

    for attr in ATTRS:
        previous_events = previous_events.withColumn("_prev_" + attr, F.lag(attr).over(order))
    previous_events = previous_events.withColumn("_event_num", F.row_number().over(order))

    same_as_prev = F.lit(True)
    for attr in ATTRS:
        same_as_prev = same_as_prev & F.col(attr).eqNullSafe(F.col("_prev_" + attr))

    changes = previous_events.where((F.col("_event_num") == 1) | (~same_as_prev))
    versions = (
        changes.withColumn("valid_from", F.col("event_time"))
        .withColumn("valid_to", F.lead("event_time").over(order))
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("snapshot_date", F.lit(run.day).cast("date"))
    )
    return add_run_metadata(versions, run).select(*SCD2_COLS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_cli_args(parser)
    args = parser.parse_args()
    run = RunInfo.from_args(args)
    spark = spark_session(f"HW23 SCD2 {run.day}")
    try:
        events = read_events(spark, run.dataset)
        versions = calculate_scd2(events, run.end, run)
        target = scd2_path(run.dataset, run.day)
        # Записываем только папку snapshot_date=D
        versions.write.mode("overwrite").parquet(target)
        saved = spark.read.parquet(target)
        print(
            f"SCD2 OK: date={run.day}, rows={saved.count()}, "
            f"users={saved.select('user_id').distinct().count()}, path={target}",
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
