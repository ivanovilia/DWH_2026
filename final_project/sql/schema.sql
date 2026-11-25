-- Schema for the online cinema final project. Statements are separated by marker lines.
CREATE TABLE IF NOT EXISTS live_events (
    event_id String,
    event_time DateTime('UTC'),
    user_id UInt64,
    event_type LowCardinality(String),
    content_id Nullable(UInt64),
    watch_time_seconds Nullable(UInt64),
    query Nullable(String),
    available_at DateTime('UTC'),
    ingested_at DateTime64(3,'UTC') DEFAULT now64(3)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time,event_id,ingested_at);

-- SPLIT --
CREATE TABLE IF NOT EXISTS kafka_events (
    event_id String,
    event_time String,
    user_id UInt64,
    event_type String,
    content_id Nullable(UInt64),
    watch_time_seconds Nullable(UInt64),
    query Nullable(String),
    available_at String
) ENGINE = Kafka
SETTINGS kafka_broker_list='kafka:19092',
         kafka_topic_list='cinema.events',
         kafka_group_name='cinema_clickhouse_live_v1',
         kafka_format='JSONEachRow',
         kafka_num_consumers=1,
         kafka_skip_broken_messages=0;

-- SPLIT --
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_live TO live_events AS
SELECT event_id,
       parseDateTimeBestEffort(event_time,'UTC') AS event_time,
       user_id, event_type, content_id, watch_time_seconds, query,
       parseDateTimeBestEffort(available_at,'UTC') AS available_at,
       now64(3) AS ingested_at
FROM kafka_events;

-- SPLIT --
-- Because retries repeat event_id, aggregate after event-id deduplication rather
-- than directly summing raw Kafka blocks. All repeated business fields are equal.
CREATE VIEW IF NOT EXISTS live_minute AS
SELECT toStartOfMinute(dedup_event_time) AS minute,
       assumeNotNull(dedup_content_id) AS content_id,
       count() AS views,
       sum(assumeNotNull(dedup_watch_time_seconds)) AS watched_seconds
FROM (
    SELECT e.event_id AS event_id,
           any(e.event_time) AS dedup_event_time,
           any(e.content_id) AS dedup_content_id,
           any(e.watch_time_seconds) AS dedup_watch_time_seconds
    FROM live_events AS e
    WHERE e.event_type = 'view'
      AND isNotNull(e.content_id)
      AND isNotNull(e.watch_time_seconds)
    GROUP BY e.event_id
)
GROUP BY minute,content_id;

-- SPLIT --
CREATE TABLE IF NOT EXISTS batch_minute (
    day Date, minute DateTime('UTC'), content_id UInt64,
    views UInt64, watched_seconds UInt64,
    publish_id String, _dag_id String, _run_id String, _task_id String,
    _data_interval_start DateTime('UTC'), _data_interval_end DateTime('UTC'),
    _loaded_at DateTime64(3,'UTC')
) ENGINE=ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMM(day)
ORDER BY (day,minute,content_id,publish_id);

-- SPLIT --
CREATE TABLE IF NOT EXISTS batch_daily_country_tier (
    day Date, country String, subscription_tier String,
    views UInt64, watched_seconds UInt64,
    publish_id String, _dag_id String, _run_id String, _task_id String,
    _data_interval_start DateTime('UTC'), _data_interval_end DateTime('UTC'),
    _loaded_at DateTime64(3,'UTC')
) ENGINE=ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMM(day)
ORDER BY (day,country,subscription_tier,publish_id);

-- SPLIT --
CREATE TABLE IF NOT EXISTS batch_top5 (
    day Date, rank UInt8, content_id UInt64, views UInt64, watched_seconds UInt64,
    publish_id String, _dag_id String, _run_id String, _task_id String,
    _data_interval_start DateTime('UTC'), _data_interval_end DateTime('UTC'),
    _loaded_at DateTime64(3,'UTC')
) ENGINE=ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMM(day)
ORDER BY (day,rank,publish_id);

-- SPLIT --
CREATE TABLE IF NOT EXISTS batch_search_words (
    day Date, word String, frequency UInt64,
    publish_id String, _dag_id String, _run_id String, _task_id String,
    _data_interval_start DateTime('UTC'), _data_interval_end DateTime('UTC'),
    _loaded_at DateTime64(3,'UTC')
) ENGINE=ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMM(day)
ORDER BY (day,word,publish_id);

-- SPLIT --
-- Active version for each day is changed only after *all* marts were inserted and checked.
CREATE TABLE IF NOT EXISTS published_days (
    day Date,
    publish_id String,
    _run_id String,
    revision UInt64,
    published_at DateTime64(3,'UTC')
) ENGINE=ReplacingMergeTree(revision) ORDER BY day;

-- SPLIT --
CREATE TABLE IF NOT EXISTS report_cutover (
    id UInt8,
    t DateTime('UTC'),
    revision UInt64
) ENGINE=ReplacingMergeTree(revision) ORDER BY id;

-- SPLIT --
CREATE VIEW IF NOT EXISTS published_minute AS
SELECT b.minute,b.content_id,b.views,b.watched_seconds,b.day
FROM batch_minute AS b FINAL
INNER JOIN published_days AS p FINAL ON b.day=p.day AND b.publish_id=p.publish_id;

-- SPLIT --
CREATE VIEW IF NOT EXISTS published_daily_country_tier AS
SELECT b.day,b.country,b.subscription_tier,b.views,b.watched_seconds
FROM batch_daily_country_tier AS b FINAL
INNER JOIN published_days AS p FINAL ON b.day=p.day AND b.publish_id=p.publish_id;

-- SPLIT --
CREATE VIEW IF NOT EXISTS published_top5 AS
SELECT b.day,b.rank,b.content_id,b.views,b.watched_seconds
FROM batch_top5 AS b FINAL
INNER JOIN published_days AS p FINAL ON b.day=p.day AND b.publish_id=p.publish_id;

-- SPLIT --
CREATE VIEW IF NOT EXISTS published_search_words AS
SELECT b.day,b.word,b.frequency
FROM batch_search_words AS b FINAL
INNER JOIN published_days AS p FINAL ON b.day=p.day AND b.publish_id=p.publish_id;

-- SPLIT --
-- T is inclusive on the realtime side. Both branches have same keys and metrics.
CREATE VIEW IF NOT EXISTS final_minute AS
SELECT u.minute AS minute,u.content_id AS content_id,
       sum(u.views) AS views,sum(u.watched_seconds) AS watched_seconds
FROM (
    SELECT minute,content_id,views,watched_seconds
    FROM published_minute
    WHERE minute < (SELECT t FROM report_cutover FINAL WHERE id=1 LIMIT 1)
    UNION ALL
    SELECT minute,content_id,views,watched_seconds
    FROM live_minute
    WHERE minute >= (SELECT t FROM report_cutover FINAL WHERE id=1 LIMIT 1)
) AS u
GROUP BY u.minute,u.content_id;
