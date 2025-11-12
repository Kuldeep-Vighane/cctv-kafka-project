# consumer_analytics.py
# Consume telemetry and events to compute a simple metric: events per minute per camera.
#
# This example keeps a sliding window in memory. For production use, stream-processing frameworks
# (Kafka Streams, Flink) or time-series DBs are recommended.

import json
import time
from kafka import KafkaConsumer
from collections import defaultdict, deque

KAFKA_BROKER = 'localhost:9092'

# We'll read camera_events for analytics in this simple demo
consumer = KafkaConsumer(
    'camera_events',
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    group_id='analytics-service',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Keep a sliding window (timestamps) for each camera to count recent events
events_window = defaultdict(lambda: deque())
WINDOW_SECONDS = 60

print('Analytics consumer started — computing events/min per camera')
for msg in consumer:
    ev = msg.value
    cam = ev['camera_id']
    now = int(time.time())
    # Push event timestamp
    events_window[cam].append(now)

    # Pop old events outside the time window
    while events_window[cam] and (now - events_window[cam][0] > WINDOW_SECONDS):
        events_window[cam].popleft()

    count = len(events_window[cam])
    print(f"Analytics: camera={cam} events_last_60s={count}")
