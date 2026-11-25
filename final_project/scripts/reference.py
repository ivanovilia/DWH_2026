"""Independent stdlib reference calculation for the provided JSONL datasets.

Used to verify all Spark/ClickHouse published values on the *fully replayed*
fixture or demo. Does not import Spark or the project's transformation code.
"""
from __future__ import annotations

import argparse
import collections
import json
from bisect import bisect_right
from pathlib import Path


def rows(path):
    return [json.loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


def unique_by_id(items, id_column):
    # Contract guarantees equal business fields for each repeated ID.
    selected = {}
    for row in items:
        identifier = row[id_column]
        if identifier not in selected or row["available_at"] < selected[identifier]["available_at"]:
            selected[identifier] = row
    return list(selected.values())


def compute(dataset):
    root = Path(dataset)
    catalogue = rows(root / "catalogue.jsonl")
    by_content = {item["content_id"]: item for item in catalogue}
    assert len(by_content) == len(catalogue)
    events = unique_by_id(rows(root / "events.jsonl"), "event_id")
    changes = unique_by_id(rows(root / "profile_changes.jsonl"), "change_id")
    by_user = collections.defaultdict(list)
    for item in changes:
        by_user[item["user_id"]].append(item)
    versions = {}
    for user, history in by_user.items():
        compressed = []
        for row in sorted(history, key=lambda r: (r["effective_at"], r["change_id"])):
            value = (row["country"], row["subscription_tier"])
            if not compressed or compressed[-1][1] != value:
                compressed.append((row["effective_at"], value))
        versions[user] = compressed

    minute = collections.defaultdict(lambda: [0, 0])
    daily = collections.defaultdict(lambda: [0, 0])
    content_daily = collections.defaultdict(lambda: [0, 0])
    words = collections.Counter()
    events_per_day = collections.Counter()
    for row in events:
        day = row["event_time"][:10]
        events_per_day[(day, row["event_type"])] += 1
        if row["event_type"] == "search":
            for word in (row.get("query") or "").lower().split():
                if len(word) > 4:
                    words[(day, word)] += 1
            continue
        assert row["event_type"] == "view"
        assert row["content_id"] in by_content, row
        history = versions[row["user_id"]]
        index = bisect_right([r[0] for r in history], row["event_time"]) - 1
        assert index >= 0, row
        country, tier = history[index][1]
        minute_key = (row["event_time"][:16] + ":00", row["content_id"])
        for table, key in ((minute, minute_key),
                           (daily, (day, country, tier)),
                           (content_daily, (day, row["content_id"]))):
            table[key][0] += 1
            table[key][1] += row["watch_time_seconds"]
    days = sorted({day for day, kind in events_per_day})
    top5 = {}
    for day in days:
        options = [(key[1], value) for key, value in content_daily.items() if key[0] == day]
        for rank, (content_id, numbers) in enumerate(sorted(options, key=lambda x: (-x[1][0], x[0]))[:5], 1):
            top5[(day, rank)] = (content_id, *numbers)
    return {"dataset": str(dataset), "users": len(versions),
            "scd2_rows": sum(map(len, versions.values())),
            "events": len(events),
            "events_per_day": {f"{day}/{kind}": count for (day, kind), count in sorted(events_per_day.items())},
            "minute": dict(minute), "daily": dict(daily), "top5": top5, "words": dict(words)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default="data/fixture")
    args = parser.parse_args()
    report = compute(args.dataset)
    print(f"REFERENCE dataset={args.dataset}, users={report['users']}, scd2_versions={report['scd2_rows']}, unique_events={report['events']}")
    print("Events/day:", report["events_per_day"])
    print("Minute keys:", len(report["minute"]))
    print("Daily country/tier keys:", len(report["daily"]))
    print("Top-5 keys:", len(report["top5"]))
    print("Word keys:", len(report["words"]))
    print("Total views:", sum(v[0] for v in report["minute"].values()))
    print("Total watched seconds:", sum(v[1] for v in report["minute"].values()))


if __name__ == "__main__":
    main()
