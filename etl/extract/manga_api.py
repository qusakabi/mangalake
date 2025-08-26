from __future__ import annotations
import datetime as dt
from typing import List, Dict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from etl.config import settings
from etl.clients.minio_client import upload_bytes
from etl.utils.jsonl import dumps_bytes

def _make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=max(0, settings.request_retries),
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def _request_page_from(base_url: str, limit: int, offset: int, session: requests.Session, tolerate_400: bool = False) -> List[Dict]:
    params = {"limit": limit, "offset": offset}
    resp = session.get(base_url, params=params, timeout=settings.request_timeout)
    # Some public APIs (e.g. MangaDex) return 400 when offset exceeds total. Treat that as "no more data".
    if tolerate_400 and resp.status_code == 400:
        return []
    # Safety: also catch raise_for_status 400 and treat it as end-of-data if tolerate_400 is True
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        if tolerate_400 and getattr(e, "response", None) is not None and e.response.status_code == 400:
            return []
        raise
    body = resp.json()
    if isinstance(body, dict) and "data" in body:
        return body["data"]
    if isinstance(body, list):
        return body
    return [body]

def _request_page(limit: int, offset: int) -> List[Dict]:
    """
    Try primary API first; if it fails (DNS/connect/HTTP), fall back to public API
    specified by settings.manga_api_fallback to keep the pipeline runnable.
    """
    session = _make_session()
    last_err: Exception | None = None
    # 1) Primary (Manga Hook)
    if settings.manga_api_base:
        try:
            return _request_page_from(settings.manga_api_base, limit, offset, session, tolerate_400=False)
        except Exception as e:
            print(f"[extract] primary API failed: {e!r}, will try fallback if configured")
            last_err = e
    # 2) Fallback (MangaDex by default) — tolerate 400 when beyond total
    if settings.manga_api_fallback:
        try:
            return _request_page_from(settings.manga_api_fallback, limit, offset, session, tolerate_400=True)
        except requests.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status == 400:
                # Treat 400 as "no more data" for public API pagination edge
                return []
            print(f"[extract] fallback API failed: {e!r}")
            last_err = e
        except Exception as e:
            print(f"[extract] fallback API failed: {e!r}")
            last_err = e
    # If we reached here, raise whatever we have
    if last_err:
        raise last_err
    raise RuntimeError("No API endpoint configured. Set MANGA_API_BASE or MANGA_API_FALLBACK.")

def fetch_and_store_jsonl(ds: str, page_size: int = 100) -> None:
    """
    Extracts manga list from API and stores as JSONL into MinIO:
      raw/manga/load_date=YYYY-MM-DD/manga_YYYYMMDD_HHMMSS.jsonl
    """
    load_date = dt.datetime.strptime(ds, "%Y-%m-%d").date()
    prefix = f"raw/manga/load_date={load_date.isoformat()}/"
    timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"{prefix}manga_{timestamp}.jsonl"

    offset = 0
    all_items: List[Dict] = []
    while True:
        items = _request_page(page_size, offset)
        if not items:
            break
        all_items.extend(items)
        if len(items) < page_size:
            break
        offset += page_size

    payload = dumps_bytes(all_items)
    upload_bytes(key, payload, "application/jsonl")
