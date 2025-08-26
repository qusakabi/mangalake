from __future__ import annotations
import json
from typing import Iterable

def dumps_bytes(records: Iterable[dict]) -> bytes:
    lines = []
    for r in records:
        lines.append(json.dumps(r, ensure_ascii=False))
    return ("\n".join(lines)).encode("utf-8")
