from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

"""
DAG: raw_from_api_to_s3
- Extracts manga data from Manga Hook API (or fallback) and stores JSONL in MinIO (S3-compatible)
- Output prefix: raw/manga/load_date=YYYY-MM-DD/manga_YYYYMMDD_HHMMSS.jsonl
"""

def extract_raw(**context):
    from etl.extract.manga_api import fetch_and_store_jsonl
    fetch_and_store_jsonl(context["ds"], page_size=100)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_from_api_to_s3",
    description="Extract manga from API to S3 (MinIO) as JSONL",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["raw", "api", "s3", "minio", "manga"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_raw",
        python_callable=extract_raw,
        doc_md="Fetch pages from API and drop JSONL files to MinIO",
    )