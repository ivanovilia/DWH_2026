"""Показать физический план JOIN из ДЗ"""
from datetime import datetime, timezone

from hw23_common import RunInfo, read_campaigns, scd2_path, spark_session
from build_campaign_recipients import calculate_recipients


run = RunInfo(
    dataset="demo",
    dag_id="hw23_daily_scd2_campaigns",
    run_id="scheduled__2026-01-23T00:00:00+00:00",
    task_id="build_campaign_recipients",
    start=datetime(2026, 1, 23, tzinfo=timezone.utc),
    end=datetime(2026, 1, 24, tzinfo=timezone.utc),
    loaded_at=datetime.now(timezone.utc),
)

spark = spark_session("DWH HW2.3: demonstrate physical JOIN plan")
spark.conf.set("spark.sql.adaptive.enabled", "false")

try:
    scd2 = spark.read.parquet(scd2_path(run.dataset, run.day))
    campaigns = read_campaigns(spark, run.dataset)
    recipients = calculate_recipients(scd2, campaigns, run)

    print("\n==== PHYSICAL JOIN PLAN (2026-01-23) ====", flush=True)
    recipients.explain(mode="formatted")
    print("\n==== END OF PLAN ====", flush=True)
    print("План сформирован без записи в S3 и без повторного запуска DAG.", flush=True)
finally:
    spark.stop()
