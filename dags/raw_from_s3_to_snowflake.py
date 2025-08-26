from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.sensors.external_task import ExternalTaskSensor

"""
DAG: raw_from_s3_to_snowflake
- Reads raw JSONL from MinIO for given ds
- Transforms to normalized DataFrame
- Loads into Snowflake ODS (ODS_MANGA)

Designed to mirror the "raw_from_s3_to_pg" pattern from the reference project, adapted to Snowflake.
"""

# Sensor: wait for raw JSONL to land in MinIO for the current ds
def is_raw_available(ds: str, **context) -> bool:
    from etl.clients.minio_client import list_keys
    prefix = f"raw/manga/load_date={ds}/"
    try:
        return len(list_keys(prefix)) > 0
    except Exception:
        # In case MinIO is temporarily unreachable, keep poking
        return False

def ensure_snowflake(**context):
    # Create WH/DB/Schema and tables if not exist
    from etl.load.snowflake_load import ensure_snowflake_objects
    ensure_snowflake_objects()

def transform_to_df(**context):
    # Returns row count for UI visibility
    from etl.transform.manga_transform import transform_latest_to_df
    ds = context["ds"]
    df = transform_latest_to_df(ds)
    return int(df.shape[0])

def load_ods(**context):
    from etl.transform.manga_transform import transform_latest_to_df
    from etl.load.snowflake_load import load_ods_manga
    ds = context["ds"]
    df = transform_latest_to_df(ds)
    load_ods_manga(df, ds)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_from_s3_to_snowflake",
    description="Load ODS from MinIO raw into Snowflake",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ods", "s3", "snowflake", "manga"],
) as dag:

    wait_upstream = ExternalTaskSensor(
        task_id="wait_for_raw_dag",
        external_dag_id="raw_from_api_to_s3",
        external_task_id="extract_raw",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
        doc_md="Wait for upstream raw DAG to finish for the same logical date",
    )

    wait_raw = PythonSensor(
        task_id="wait_for_raw",
        python_callable=is_raw_available,
        op_kwargs={"ds": "{{ ds }}"},
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
        doc_md="Wait until raw files for the current ds appear in MinIO",
    )

    t0 = PythonOperator(task_id="ensure_snowflake", python_callable=ensure_snowflake, doc_md="Create objects in Snowflake")
    t1 = PythonOperator(task_id="transform_to_df", python_callable=transform_to_df, doc_md="Normalize raw JSON into a DataFrame")
    t2 = PythonOperator(task_id="load_ods", python_callable=load_ods, doc_md="Upsert into ODS_MANGA")

    wait_upstream >> wait_raw >> t0 >> t1 >> t2