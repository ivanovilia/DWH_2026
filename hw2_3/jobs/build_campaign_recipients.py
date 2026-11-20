"""Задача Airflow 2- исторические получатели рассылок из снимка SCD2 той же даты"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, functions as F

from hw23_common import (
    RECIPIENT_COLS, RunInfo, add_cli_args, add_run_metadata, read_campaigns,
    recipients_path, scd2_path, spark_session,
)


def calculate_recipients(scd2: DataFrame, campaigns: DataFrame, run: RunInfo) -> DataFrame:
    # Рассылки относятся только к текущему интервалу [start, end)
    c = campaigns.where(
        (F.col("send_at") >= F.lit(run.start)) & (F.col("send_at") < F.lit(run.end))
    ).alias("c")
    v = scd2.alias("v")

    # Небольшой справочник кампаний рассылаем исполнителям
    condition = (
        (F.lower(F.substring_index(F.col("v.user_email"), "@", -1)) == F.lower(F.col("c.email_domain")))
        & (F.col("v.valid_from") <= F.col("c.send_at"))
        & (F.col("v.valid_to").isNull() | (F.col("c.send_at") < F.col("v.valid_to")))
    )
    joined = v.join(F.broadcast(c), condition, "inner")
    result = joined.select(
        F.col("c.campaign_id").alias("campaign_id"),
        F.col("c.campaign_name").alias("campaign_name"),
        F.col("c.send_at").alias("send_at"),
        F.col("c.email_domain").alias("email_domain"),
        F.col("v.user_id").alias("user_id"),
        F.col("v.user_fio").alias("user_fio"),
        F.col("v.user_email").alias("user_email"),
        F.col("v.user_login").alias("user_login"),
        F.col("v.valid_from").alias("user_valid_from"),
        F.col("v.valid_to").alias("user_valid_to"),
        F.lit(run.day).cast("date").alias("calculation_date"),
    )
    return add_run_metadata(result, run).select(*RECIPIENT_COLS)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_cli_args(parser)
    args = parser.parse_args()
    run = RunInfo.from_args(args)
    spark = spark_session(f"HW23 recipients {run.day}")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    try:
        scd2 = spark.read.parquet(scd2_path(run.dataset, run.day))
        campaigns = read_campaigns(spark, run.dataset)
        recipients = calculate_recipients(scd2, campaigns, run)
        target = recipients_path(run.dataset, run.day)

        print("JOIN PHYSICAL PLAN:", flush=True)
        recipients.explain(mode="formatted")
        recipients.write.mode("overwrite").parquet(target)
        saved = spark.read.parquet(target)
        print(
            f"RECIPIENTS OK: date={run.day}, rows={saved.count()}, "
            f"campaigns={saved.select('campaign_id').distinct().count()}, path={target}",
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
