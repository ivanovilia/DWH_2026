"""Граничные случаи из fixtures"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from build_campaign_recipients import calculate_recipients
from build_scd2 import calculate_scd2
from hw23_common import RunInfo, read_campaigns, read_events, spark_session


# Ожидаемые значения для отдельных рассылок из учебного fixtures
EXPECTED = {
    "2026-01-23": {101: [1001], 102: [1002], 103: [1001], 104: []},
    "2026-01-24": {105: [1001, 1002], 106: [1003], 107: []},
    "2026-01-25": {108: [1002], 109: [1001], 110: [1004], 113: [], 115: [1001], 116: [1001]},
}
VERSIONS_BY_DATE = {"2026-01-23": 3, "2026-01-24": 6, "2026-01-25": 10}


def main() -> None:
    spark = spark_session("HW23 fixture edge cases")
    try:
        events = read_events(spark, "fixtures")
        campaigns = read_campaigns(spark, "fixtures")
        for day, by_campaign in EXPECTED.items():
            start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
            end = start + timedelta(days=1)
            run = RunInfo("fixtures", "fixture_test", "fixture_test", "fixture_test", start, end, datetime.now(timezone.utc))
            scd = calculate_scd2(events, end, run).cache()
            count = scd.count()
            if count != VERSIONS_BY_DATE[day]:
                raise AssertionError(f"{day}: SCD2 {count} != {VERSIONS_BY_DATE[day]}")
            recipients = calculate_recipients(scd, campaigns, run)
            for campaign_id, expected_users in by_campaign.items():
                actual = sorted(r.user_id for r in recipients.where(F.col("campaign_id") == campaign_id).select("user_id").collect())
                if actual != expected_users:
                    raise AssertionError(f"{day}: campaign {campaign_id}: {actual} != {expected_users}")
                print(f"OK: {day}, campaign_id={campaign_id}, users={actual}", flush=True)
            scd.unpersist()
        start = datetime(2026, 1, 27, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        run = RunInfo("fixtures", "fixture_test", "fixture_test", "fixture_test", start, end, datetime.now(timezone.utc))
        empty = calculate_recipients(calculate_scd2(events, end, run), campaigns, run)
        if empty.count() != 0:
            raise AssertionError("Expected empty recipients for fixture on 2026-01-27")
        print(f"OK: empty result 2026-01-27 has {len(empty.schema.fields)} typed columns", flush=True)
        print("FIXTURE TESTS PASSED: boundaries, historical email, repeats, empty result, return to old login", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
