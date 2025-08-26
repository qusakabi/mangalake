from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Task callables
def ensure_snowflake(**context):
    from etl.load.snowflake_load import ensure_snowflake_objects
    ensure_snowflake_objects()

def extract_raw(**context):
    from etl.extract.manga_api import fetch_and_store_jsonl
    fetch_and_store_jsonl(context["ds"])

def transform_to_df(**context):
    from etl.transform.manga_transform import transform_latest_to_df
    # returns row count for XCom/visibility
    return int(transform_latest_to_df(context["ds"]).shape[0])

def load_ods(**context):
    from etl.transform.manga_transform import transform_latest_to_df
    from etl.load.snowflake_load import load_ods_manga
    ds = context["ds"]
    df = transform_latest_to_df(ds)
    load_ods_manga(df, ds)

def build_dm(**context):
    from etl.load.snowflake_load import build_dm_all
    build_dm_all(context["ds"])

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="manga_lakehouse_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["manga", "lakehouse", "minio", "snowflake"],
) as dag:

    t0 = PythonOperator(task_id="ensure_snowflake", python_callable=ensure_snowflake)
    t1 = PythonOperator(task_id="extract_raw", python_callable=extract_raw)
    t2 = PythonOperator(task_id="transform_to_df", python_callable=transform_to_df)
    t3 = PythonOperator(task_id="load_ods", python_callable=load_ods)
    t4 = PythonOperator(task_id="build_dm", python_callable=build_dm)

    t0 >> t1 >> t2 >> t3 >> t4
