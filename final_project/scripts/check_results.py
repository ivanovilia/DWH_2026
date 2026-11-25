"""Сверяет витрины ClickHouse с эталонным расчётом на стандартной библиотеке Python после полной повторной отправки данных и публикации витрин."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from ch_client import query
from reference import compute


def fetch(sql):
    return [json.loads(line) for line in query(sql + " FORMAT JSONEachRow").splitlines() if line]


def normal_time(text):
    return text.replace("T", " ").replace("Z", "")[:19]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=["fixture", "demo"], default="fixture")
    args = parser.parse_args()
    ref = compute(f"/opt/airflow/data/{args.dataset}")
    minute = {(normal_time(r["minute"]), int(r["content_id"])):
              [int(r["views"]), int(r["watched_seconds"])]
              for r in fetch("SELECT minute,content_id,views,watched_seconds FROM final_minute")}
    expected_minute = {(normal_time(k[0]), k[1]): v for k, v in ref["minute"].items()}
    assert minute == expected_minute, f"MINUTE DIFF actual={len(minute)} reference={len(expected_minute)}"
    daily = {(r["day"], r["country"], r["subscription_tier"]):
             [int(r["views"]), int(r["watched_seconds"])]
             for r in fetch("SELECT * FROM published_daily_country_tier")}
    assert daily == ref["daily"], "DAILY COUNTRY/TIER DIFF"
    top = {(r["day"], int(r["rank"])): (int(r["content_id"]), int(r["views"]), int(r["watched_seconds"]))
           for r in fetch("SELECT * FROM published_top5")}
    assert top == ref["top5"], "TOP5 DIFF"
    words = {(r["day"], r["word"]): int(r["frequency"])
             for r in fetch("SELECT * FROM published_search_words")}
    assert words == ref["words"], "WORDS DIFF"
    cutoff = query("SELECT toString(t) FROM report_cutover FINAL WHERE id=1").strip()
    print(f"ALL INDEPENDENT CHECKS PASSED dataset={args.dataset} cutover={cutoff}")
    print(f"Minute keys: {len(minute)}; daily keys: {len(daily)}; top-5 keys: {len(top)}; words: {len(words)}")


if __name__ == "__main__":
    main()
