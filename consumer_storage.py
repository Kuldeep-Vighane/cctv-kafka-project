# consumer_storage.py
# Consume video metadata and simulate saving it to a persistent store (here -- a local CSV).
#
# This simulates recording metadata (pointers to heavy video files) in a DB for search.

import csv
import os
import json
from kafka import KafkaConsumer

KAFKA_BROKER = 'localhost:9092'
OUT_FILE = 'video_metadata_store.csv'

consumer = KafkaConsumer(
    'video_metadata',
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    group_id='storage-service',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Ensure file exists with header
if not os.path.exists(OUT_FILE):
    with open(OUT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['video_id', 'camera_id', 'ts', 'duration_sec', 's3_key', 'related_event_id'])

print('Storage consumer started — appending metadata to', OUT_FILE)
for msg in consumer:
    md = msg.value
    # Append metadata to CSV (simulates recording to DB)
    with open(OUT_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([md['video_id'], md['camera_id'], md['ts'], md['duration_sec'], md['s3_key'], md.get('related_event_id')])
    print('Stored metadata for', md['video_id'])
