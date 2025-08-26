from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

"""
DAG: fct_count_day_manga
- Aggregates daily counts by status into Snowflake table DM_MANGA_DAILY_COUNTS
- Mirrors reference pattern "fct_count_day_earthquake" but for manga domain and Snowflake
"""

# Wait for upstream ODS load task (same logical date) via ExternalTaskSensor

def ensure_snowflake(**context):
    from etl.load.snowflake_load import ensure_snowflake_objects
    ensure_snowflake_objects()

def build_counts(**context):
    from etl.load.snowflake_load import build_dm_manga_daily_counts
    build_dm_manga_daily_counts(context["ds"])

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fct_count_day_manga",
    description="DM: daily counts of manga by status (Snowflake)",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dm", "snowflake", "manga"],
) as dag:

    wait_ods = ExternalTaskSensor(
        task_id="wait_for_ods",
        external_dag_id="raw_from_s3_to_snowflake",
        external_task_id="load_ods",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=60,
        timeout=60 * 60,  # 1h
        mode="poke",
        doc_md="Wait for ODS load to finish for the same logical date",
    )

    t0 = PythonOperator(task_id="ensure_snowflake", python_callable=ensure_snowflake, doc_md="Create Snowflake objects if missing")
    t1 = PythonOperator(task_id="build_daily_counts", python_callable=build_counts, doc_md="Insert daily counts into DM_MANGA_DAILY_COUNTS")

    wait_ods >> t0 >> t1