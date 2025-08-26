from __future__ import annotations
import io
from typing import Iterable, List
import boto3
from botocore.config import Config as BotoConfig
from etl.config import settings

_session = boto3.session.Session()
_s3 = _session.client(
    "s3",
    endpoint_url=settings.minio_endpoint,
    aws_access_key_id=settings.minio_access_key,
    aws_secret_access_key=settings.minio_secret_key,
    config=BotoConfig(s3={"addressing_style": "path"}),
)

BUCKET = settings.minio_bucket

def upload_bytes(key: str, data: bytes, content_type: str = "application/json") -> None:
    _s3.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType=content_type)

def list_keys(prefix: str) -> List[str]:
    keys: List[str] = []
    continuation = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix}
        if continuation:
            kw["ContinuationToken"] = continuation
        resp = _s3.list_objects_v2(**kw)
        for it in resp.get("Contents", []):
            keys.append(it["Key"])
        if resp.get("IsTruncated"):
            continuation = resp.get("NextContinuationToken")
        else:
            break
    return keys

def read_bytes(key: str) -> bytes:
    obj = _s3.get_object(Bucket=BUCKET, Key=key)
    return obj["Body"].read()

def upload_csv_bytes(key: str, data: bytes) -> None:
    upload_bytes(key, data, "text/csv")
