"""Независимая группа потребителей Kafka сохраняет каждое сообщение в S3 до фиксации смещения.
"""
from __future__ import annotations

import os
import signal
import time
import uuid
from collections import defaultdict
from io import BytesIO

import boto3
from botocore.config import Config
from confluent_kafka import Consumer, TopicPartition

RUNNING = True


def stop(*_):
    global RUNNING
    RUNNING = False


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    kafka = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "group.id": os.environ.get("KAFKA_ARCHIVE_GROUP", "cinema_s3_archive_v1"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 600000,
    })
    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                      config=Config(s3={"addressing_style": "path"}))
    bucket = os.environ["S3_BUCKET"]
    dataset = os.environ.get("FP_DATASET", "fixture")
    topics = {"cinema.events": "events", "cinema.profile_changes": "profiles"}
    kafka.subscribe(list(topics))
    buffers = defaultdict(list)
    last_flush = time.monotonic()

    def flush(topic, partition):
        entries = buffers[(topic, partition)]
        if not entries:
            return
        first, last = entries[0][0], entries[-1][0]
        key = (f"raw/{dataset}/{topics[topic]}/partition={partition}/"
               f"offset_{first:012d}_{last:012d}_{uuid.uuid4().hex}.jsonl")
        body = b"\n".join(value for _, value in entries) + b"\n"
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")
        kafka.commit(offsets=[TopicPartition(topic, partition, last + 1)], asynchronous=False)
        print(f"ARCHIVED topic={topic} partition={partition} count={len(entries)} key={key}", flush=True)
        buffers[(topic, partition)].clear()

    try:
        while RUNNING:
            msg = kafka.poll(1.0)
            if msg is not None:
                if msg.error():
                    raise RuntimeError(msg.error())
                if msg.topic() in topics and msg.value():
                    key = (msg.topic(), msg.partition())
                    buffers[key].append((msg.offset(), msg.value()))
                    if len(buffers[key]) >= 100:
                        flush(*key)
            if time.monotonic() - last_flush >= 2.0:
                for key in list(buffers):
                    flush(*key)
                last_flush = time.monotonic()
    finally:
        for key in list(buffers):
            flush(*key)
        kafka.close()


if __name__ == "__main__":
    main()
