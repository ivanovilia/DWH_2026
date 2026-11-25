"""Final DWH: scheduled UTC processing of 2026-01-01..2026-01-03.

The DAG is initially paused. Replay the first event-arrival phase and wait for
both Kafka consumers before unpausing, otherwise the snapshot may be incomplete.
Use Trigger DAG + {"day":"2026-01-01"} to recompute a historical day.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable

DAG_ID = "cinema_daily_dwh"


def execute_stage(stage: str, **context):
    dag_run = context["dag_run"]
    configuration = dag_run.conf or {}
    day = (configuration["day"] if configuration.get("day") else
           context["data_interval_start"].in_timezone("UTC").date().isoformat())
    # Do not allow arbitrary directory traversal via dag_run.conf.
    from datetime import date
    date.fromisoformat(day)
    cmd = [sys.executable, "/opt/airflow/jobs/pipeline.py", stage,
           "--day", day,
           "--run-id", dag_run.run_id,
           "--dag-id", context["dag"].dag_id,
           "--task-id", context["task"].task_id,
           "--dataset", os.environ.get("FP_DATASET", "fixture")]
    print("EXEC", " ".join(cmd), flush=True)
    # Pipe child stdout through Airflow logging so the actual Spark plan and
    # validation messages remain visible on the task's Logs tab.
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, bufsize=1) as process:
        for line in process.stdout:
            print(line.rstrip(), flush=True)
        return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"Pipeline stage {stage} exited with {return_code}")


with DAG(
    dag_id=DAG_ID,
    description="Kafka archive -> Spark SCD2 & marts -> validated ClickHouse publication",
    timetable=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 3, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["dwh", "cinema", "kafka", "spark", "scd2", "clickhouse"],
) as dag:
    tasks = {
        stage: PythonOperator(task_id=task_name,
                              python_callable=execute_stage,
                              op_kwargs={"stage": stage})
        for stage, task_name in [
            ("snapshot", "snapshot_inputs"),
            ("scd2", "build_profile_scd2"),
            ("marts", "calculate_marts"),
            ("validate", "validate_and_compare_rdd"),
            ("publish", "publish_to_clickhouse"),
        ]
    }
    (tasks["snapshot"] >> tasks["scd2"] >> tasks["marts"]
     >> tasks["validate"] >> tasks["publish"])
