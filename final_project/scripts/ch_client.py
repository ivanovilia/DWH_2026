"""HTTP-клиент ClickHouse для инициализации, публикации данных и получения отчётов"""
from __future__ import annotations
import os
import requests


def query(sql: str, data: bytes | str | None = None, *, database=None):
    root = os.environ.get("CH_URL", "http://clickhouse:8123").rstrip("/")
    username = os.environ.get("CH_USER", "cinema")
    password = os.environ.get("CH_PASSWORD", "cinema-local-secret")
    params = {"database": database or os.environ.get("CH_DATABASE", "cinema"),
              "wait_end_of_query": 1}
    if data is None:
        payload = sql.encode("utf-8")
    else:
        payload = sql.encode("utf-8") + b"\n" + (data.encode("utf-8") if isinstance(data, str) else data)
    response = requests.post(root, params=params, data=payload,
                             auth=(username, password), timeout=120)
    if not response.ok:
        raise RuntimeError(f"ClickHouse {response.status_code}: {response.text[:3000]}\nQuery: {sql[:500]}")
    return response.text
