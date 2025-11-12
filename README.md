CCTV Kafka Streaming Demo
Author: Kuldeep Vighane
Role: Cloud Operations Engineer
Technology Stack: Python, Apache Kafka, Docker
Project Overview

This project is a demo implementation of a real-time data streaming system for CCTV camera alerts using Apache Kafka.
The goal is to simulate how a CCTV manufacturer could process camera events, alerts, and video metadata in real time using Kafka as the message broker.

No real cameras are used in this demo — instead, a Python script simulates camera devices sending messages to Kafka topics.

Purpose of the Project

The main purpose of this project is to:

Understand how Apache Kafka handles real-time data streams.

Demonstrate producer-consumer architecture using Python.

Simulate real-world use cases like motion detection and person detection alerts.

Learn how multiple independent consumers can process different types of data from Kafka topics.

Components Implemented

Kafka Setup (with Docker)

Kafka and Zookeeper are started using Docker Compose.

Three Kafka topics are created:

camera_events

video_metadata

camera_telemetry

Python Producer (producer_camera.py)

Simulates 10 virtual cameras.

Sends:

Telemetry data (temperature, uptime, etc.)

Random motion or AI detection events.

Metadata about recorded video clips.

Messages are published to Kafka topics in JSON format.

Consumers

consumer_alerts.py
Reads messages from camera_events topic and prints alerts when “person_detected” events are received with high confidence.

consumer_storage.py
Reads video metadata from video_metadata topic and stores it locally in a CSV file.

consumer_analytics.py
Reads events from camera_events and counts the number of events received per minute per camera.

How to Run

Start Kafka and Zookeeper

docker compose up -d


Verify containers:

docker compose ps


Create Kafka Topics

docker compose exec kafka bash
kafka-topics --bootstrap-server localhost:9092 --create --topic camera_telemetry --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic camera_events --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic video_metadata --partitions 3 --replication-factor 1
exit


Setup Python Environment

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt


Run the Consumers (in separate terminals)

python consumer_alerts.py
python consumer_storage.py
python consumer_analytics.py


Run the Producer

python producer_camera.py


You will see alerts, metadata logs, and analytics outputs in the respective terminals.

Testing the System

To manually test the alert system, a demo script (test_send_event.py) can be used to send a sample “person_detected” event to Kafka:

import json, time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event = {
    'event_id': 'TEST-EVENT-1',
    'camera_id': 'CAM_TEST_001',
    'ts': int(time.time()),
    'type': 'person_detected',
    'confidence': 0.95
}

producer.send('camera_events', value=event)
producer.flush()
producer.close()
print('Test event sent.')


Run it:

python test_send_event.py


Expected output in consumer_alerts.py:

ALERT: Person at CAM_TEST_001 ts=1730000000 conf=0.95

Errors Faced and Solutions
Issue	Cause	Solution
NoBrokersAvailable	Kafka not started or wrong port	Ensure Kafka container is running and port 9092 is accessible
UnknownTopicOrPartitionError	Topics not created	Created topics manually inside Kafka container
No alerts shown	Random events didn’t include person detection	Created test_send_event.py to send manual alert
CSV file missing	Consumer started in wrong directory	Always run consumer_storage.py from project root
Producer stopped unexpectedly	Interrupted manually (Ctrl+C)	Restart producer to continue generating messages
Project Usability

This project demonstrates:

How Kafka handles real-time data streaming between multiple producers and consumers.

How Python can be used for message-based processing.

A basic structure similar to IoT or CCTV alert systems.

It can be extended to:

Add real camera integrations or APIs.

Connect dashboards for live alert visualization.

Use databases for metadata storage instead of CSV.

Notes

This is a simulation project — all camera data is randomly generated.

No real camera devices are used or connected.

It’s meant purely for educational and testing purposes.

Author

Kuldeep Vighane
Cloud Operations Engineer
📍 Aurangabad, Maharashtra
📧 nishupvighane@gmail.com