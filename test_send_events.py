import json, time
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

event = {
    'event_id': 'TEST-EVENT-1',
    'camera_id': 'CAM_TEST_001',
    'ts': int(time.time()),
    'type': 'person_detected',
    'confidence': 0.95,
    'thumbnail_url': 'https://example.com/test-thumb.jpg'
}

producer.send('camera_events', value=event)
producer.flush()
print("Test event sent!")
