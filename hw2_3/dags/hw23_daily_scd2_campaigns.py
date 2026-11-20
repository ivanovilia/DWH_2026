"""Три ежедневных scheduled DAG runs 23–25 января 2026"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable

DAG_ID = "hw23_daily_scd2_campaigns"


def run_spark_job(script: str, **context) -> None:
    """Airflow передаёт общий для двух задач интервал и run_id."""
    start = context["data_interval_start"].in_timezone("UTC")
    end = context["data_interval_end"].in_timezone("UTC")
    if end - start != timedelta(days=1):
        raise ValueError(f"Ожидается суточный интервал: {start} / {end}")

    task = context["task"]
    dag_run = context["dag_run"]
    command = [
        sys.executable,
        f"/opt/airflow/jobs/{script}",
        "--dataset", os.environ.get("HW23_DATASET", "demo"),
        "--dag-id", context["dag"].dag_id,
        "--run-id", dag_run.run_id,
        "--task-id", task.task_id,
        "--start", start.isoformat(),
        "--end", end.isoformat(),
    ]
    print(
        f"RUN job={script} interval=[{start}, {end}) "
        f"run_id={dag_run.run_id} run_type={dag_run.run_type}",
        flush=True,
    )
    # Не запускаем Spark при парсинге DAG: только внутри задачи Airflow.
    subprocess.run(command, check=True)


with DAG(
    dag_id=DAG_ID,
    description="Ежедневный SCD2 и исторические получатели рассылок",
    timetable=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 1, 23, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 25, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["dwh", "hw23", "scd2", "spark"],
) as dag:
    build_user_scd2 = PythonOperator(
        task_id="build_user_scd2",
        python_callable=run_spark_job,
        op_kwargs={"script": "build_scd2.py"},
    )
    build_campaign_recipients = PythonOperator(
        task_id="build_campaign_recipients",
        python_callable=run_spark_job,
        op_kwargs={"script": "build_campaign_recipients.py"},
    )
    build_user_scd2 >> build_campaign_recipients
