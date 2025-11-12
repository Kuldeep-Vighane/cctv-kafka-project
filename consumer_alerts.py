# consumer_alerts.py
# Consume events and perform alerting logic (here we just print the alert).
#
# Important: In production, replace prints with actual alerting (SMS, push, webhook).

import json
from kafka import KafkaConsumer

KAFKA_BROKER = 'localhost:9092'

consumer = KafkaConsumer(
    'camera_events',
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',  # for demo; in prod consider 'latest' and offset management
    group_id='alerts-service',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print('Alert consumer started')
for msg in consumer:
    event = msg.value
    # Simple rule: if a person detected with confidence > 0.6, then alert
    if event['type'] == 'person_detected' and event['confidence'] > 0.6:
        # In production, this is where you'd call SMS/email/push or create a ticket
        print(f"ALERT: Person at {event['camera_id']} ts={event['ts']} conf={event['confidence']}")
    else:
        # For debugging we might still log other events
        print(f"Event: {event['type']} from {event['camera_id']}")
