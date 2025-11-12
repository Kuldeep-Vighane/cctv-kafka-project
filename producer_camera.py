# producer_camera.py
# Simulate many CCTV cameras producing telemetry, events, and video metadata to Kafka.
#
# Comments are included to help you understand each step.

import json
import random
import time
import uuid
from kafka import KafkaProducer

# Kafka broker address (local dev)
KAFKA_BROKER = 'localhost:9092'

# Create a Kafka producer that serializes messages as JSON bytes.
# linger_ms helps batch small messages for efficiency.
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10
)

# Simulate 10 cameras with IDs CAM_001 ... CAM_010
CAMERAS = [f"CAM_{i:03d}" for i in range(1, 11)]

def send_telemetry(camera_id):
    """Send periodic health/telemetry message for a camera."""
    msg = {
        'camera_id': camera_id,
        'ts': int(time.time()),
        'uptime_sec': random.randint(100, 100_000),
        'storage_free_pct': random.randint(5, 95),
        'temperature_c': round(20 + random.random() * 15, 1)
    }
    producer.send('camera_telemetry', value=msg)

def send_event(camera_id):
    """Send a motion/event message — e.g., motion detected or AI inference."""
    event_types = ['motion', 'person_detected', 'vehicle_detected', 'tamper']
    evt = {
        'event_id': str(uuid.uuid4()),
        'camera_id': camera_id,
        'ts': int(time.time()),
        'type': random.choice(event_types),
        'confidence': round(random.random(), 2),
        'thumbnail_url': f"https://example.com/thumbnails/{uuid.uuid4()}.jpg"
    }
    producer.send('camera_events', value=evt)
    return evt

def send_video_metadata(camera_id, event):
    """Simulate that a short video clip was recorded and uploaded. We send metadata pointing to it."""
    md = {
        'camera_id': camera_id,
        'ts': int(time.time()),
        'video_id': str(uuid.uuid4()),
        'duration_sec': random.randint(3, 30),
        's3_key': f"videos/{camera_id}/{int(time.time())}_{random.randint(1000,9999)}.mp4",
        'related_event_id': event.get('event_id')
    }
    producer.send('video_metadata', value=md)

if __name__ == '__main__':
    print('Starting camera producer — press Ctrl+C to stop')
    try:
        while True:
            # pick a random camera
            cam = random.choice(CAMERAS)

            # Always send telemetry for the camera
            send_telemetry(cam)

            # Randomly generate an event sometimes
            if random.random() < 0.3:  # 30% chance
                evt = send_event(cam)

                # After event, simulate that a clip was uploaded
                send_video_metadata(cam, evt)

            # Flush occasionally to ensure data is sent timely
            producer.flush()

            # Sleep a bit — in real world telemetry might be every few seconds
            time.sleep(1)

    except KeyboardInterrupt:
        print('Producer stopped by user')
        producer.close()
