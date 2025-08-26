from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

"""
DAG: fct_avg_day_manga
- Computes daily average YEAR by STATUS into Snowflake table DM_MANGA_AVG_YEAR
- Mirrors reference pattern "fct_avg_day_earthquake", adapted for manga and Snowflake
"""

# Wait for upstream ODS load task (same logical date) via ExternalTaskSensor

def ensure_snowflake(**context):
    from etl.load.snowflake_load import ensure_snowflake_objects
    ensure_snowflake_objects()

def build_avg(**context):
    from etl.load.snowflake_load import build_dm_manga_avg_year
    build_dm_manga_avg_year(context["ds"])

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fct_avg_day_manga",
    description="DM: daily average publish YEAR by STATUS (Snowflake)",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dm", "avg", "snowflake", "manga"],
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

    t0 = PythonOperator(
        task_id="ensure_snowflake",
        python_callable=ensure_snowflake,
        doc_md="Create Snowflake objects if missing",
    )
    t1 = PythonOperator(
        task_id="build_daily_avg_year",
        python_callable=build_avg,
        doc_md="Insert averages into DM_MANGA_AVG_YEAR",
    )

    wait_ods >> t0 >> t1