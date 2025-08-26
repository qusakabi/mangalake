from __future__ import annotations
import datetime as dt
import json
from typing import Any, Dict, List, Optional
import pandas as pd

from etl.clients.minio_client import list_keys, read_bytes
from etl.config import settings

def _extract_title(item: Dict[str, Any]) -> Optional[str]:
    t = item.get("title")
    if isinstance(t, str):
        return t
    if isinstance(t, dict):
        for lang in ("en", "ru", "ja"):
            if t.get(lang):
                return t[lang]
        # first available
        for v in t.values():
            if isinstance(v, str):
                return v
    attr = item.get("attributes", {})
    if isinstance(attr, dict):
        t2 = attr.get("title")
        if isinstance(t2, dict):
            for lang in ("en", "ru", "ja"):
                if t2.get(lang):
                    return t2[lang]
            for v in t2.values():
                if isinstance(v, str):
                    return v
    return None

def _extract_status(item: Dict[str, Any]) -> Optional[str]:
    for k in ("status",):
        v = item.get(k)
        if isinstance(v, str):
            return v
    attr = item.get("attributes", {})
    if isinstance(attr, dict) and isinstance(attr.get("status"), str):
        return attr["status"]
    return None

def _extract_last_chapter(item: Dict[str, Any]) -> Optional[str]:
    attr = item.get("attributes", {})
    for k in ("lastChapter", "last_chapter"):
        if isinstance(item.get(k), (str, int)):
            return str(item[k])
        if isinstance(attr.get(k), (str, int)):
            return str(attr[k])
    return None

def _extract_year(item: Dict[str, Any]) -> Optional[int]:
    attr = item.get("attributes", {})
    for k in ("year", "publishYear"):
        v = item.get(k, attr.get(k))
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            try:
                return int(v)
            except Exception:
                return None
    return None

def _extract_tags(item: Dict[str, Any]) -> Optional[str]:
    # Many APIs keep tags under attributes.tags: [{attributes: {name: {en: '...'}}}]
    tags = []
    attr = item.get("attributes", {})
    cand = item.get("tags") or attr.get("tags")
    if isinstance(cand, list):
        for t in cand:
            if isinstance(t, dict):
                name = t.get("name")
                if isinstance(name, str):
                    tags.append(name)
                    continue
                attr2 = t.get("attributes", {})
                nm = attr2.get("name")
                if isinstance(nm, dict):
                    tags.append(nm.get("en") or nm.get("ru") or next(iter(nm.values()), None))
    tags = [x for x in tags if x]
    return ", ".join(tags) if tags else None

def _extract_updated_at(item: Dict[str, Any]) -> Optional[str]:
    for path in (("updatedAt",), ("attributes","updatedAt")):
        ref = item
        ok = True
        for k in path:
            if isinstance(ref, dict) and k in ref:
                ref = ref[k]
            else:
                ok = False
                break
        if ok and isinstance(ref, str):
            return ref
    return None

def _extract_id(item: Dict[str, Any]) -> str:
    for k in ("id", "mangaId", "manga_id", "uuid"):
        v = item.get(k)
        if isinstance(v, (str, int)):
            return str(v)
    return json.dumps(item, sort_keys=True)[:64]  # fallback

def _load_raw_records(prefix: str) -> List[Dict[str, Any]]:
    keys = list_keys(prefix)
    records: List[Dict[str, Any]] = []
    for k in keys:
        if not k.endswith(".jsonl"):
            continue
        data = read_bytes(k).decode("utf-8")
        for line in data.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                pass
    return records

def transform_latest_to_df(ds: str) -> pd.DataFrame:
    """Reads latest raw for a given ds from MinIO and returns a normalized DataFrame."""
    load_date = dt.datetime.strptime(ds, "%Y-%m-%d").date()
    prefix = f"raw/manga/load_date={load_date.isoformat()}/"
    items = _load_raw_records(prefix)
    rows = []
    for it in items:
        rows.append({
            "MANGA_ID": _extract_id(it),
            "TITLE": _extract_title(it),
            "STATUS": _extract_status(it),
            "LAST_CHAPTER": _extract_last_chapter(it),
            "YEAR": _extract_year(it),
            "TAGS": _extract_tags(it),
            "UPDATED_AT": _extract_updated_at(it),
        })
    df = pd.DataFrame(rows, columns=["MANGA_ID","TITLE","STATUS","LAST_CHAPTER","YEAR","TAGS","UPDATED_AT"])
    return df
