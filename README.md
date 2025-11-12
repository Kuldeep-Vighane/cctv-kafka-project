# CCTV Kafka Streaming Demo

**Author:** Kuldeep Vighane  
**Role:** Cloud Operations Engineer  
**Location:** Aurangabad, Maharashtra  
📧 nishupvighane@gmail.com  

---

## Project Overview

This project is a demo implementation of a real-time data streaming system for CCTV camera alerts using **Apache Kafka**.  
The goal is to simulate how a CCTV manufacturer could process camera events, alerts, and video metadata in real time using Kafka as the message broker.

No real cameras are used in this demo — instead, a Python script simulates camera devices sending messages to Kafka topics.

---

## Purpose of the Project

The main purpose of this project is to:

- Understand how **Apache Kafka** handles real-time data streams.  
- Demonstrate **producer-consumer architecture** using Python.  
- Simulate real-world use cases like **motion detection** and **person detection alerts**.  
- Learn how multiple independent consumers can process different types of data from Kafka topics.  

---

## Components Implemented

### 1️⃣ Kafka Setup (with Docker)

Kafka and Zookeeper are started using **Docker Compose**.

**Kafka Topics:**
- `camera_events`
- `video_metadata`
- `camera_telemetry`

---

### 2️⃣ Python Producer (`producer_camera.py`)

- Simulates 10 virtual cameras.
- Sends:
  - Telemetry data (temperature, uptime, etc.)
  - Random motion or AI detection events
  - Metadata about recorded video clips
- Publishes messages to Kafka topics in **JSON format**

---

### 3️⃣ Consumers

| File | Description |
|------|--------------|
| **consumer_alerts.py** | Reads messages from `camera_events` and prints alerts when “person_detected” events are received with high confidence. |
| **consumer_storage.py** | Reads `video_metadata` messages and stores them locally in a CSV file. |
| **consumer_analytics.py** | Reads events from `camera_events` and counts events per minute per camera. |

---

### 4️⃣ Dashboard (`kafka_dashboard.py`)

A **FastAPI-based dashboard** to visualize Kafka data in real time.

**Features:**
- Displays live logs of all events.
- Shows temperature and telemetry charts.
- Displays per-camera event counts and stored video count.
- WebSocket-based live updates (no refresh needed).

**How to Run Dashboard:**

# Make sure Kafka is running and the producer is sending data
# Then run:
uvicorn kafka_dashboard:app --host 0.0.0.0 --port 8000 --reload
Open your browser and visit:

arduino

http://localhost:8000
You’ll see a real-time dashboard showing live Kafka data.

How to Run the Project
1️⃣ Start Kafka and Zookeeper
docker compose up -d
docker compose ps
2️⃣ Create Kafka Topics
docker compose exec kafka bash
kafka-topics --bootstrap-server localhost:9092 --create --topic camera_telemetry --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic camera_events --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic video_metadata --partitions 3 --replication-factor 1
exit
3️⃣ Setup Python Environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
4️⃣ Run the Consumers (in separate terminals)
python consumer_alerts.py
python consumer_storage.py
python consumer_analytics.py
5️⃣ Run the Producer
python producer_camera.py
6️⃣ Run the Dashboard
uvicorn kafka_dashboard:app --host 0.0.0.0 --port 8000 --reload
Then open: http://localhost:8000

Testing the System
You can send a manual test event:

python
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

makefile
ALERT: Person at CAM_TEST_001 ts=1730000000 conf=0.95
Common Issues and Solutions
Issue	Cause	Solution
NoBrokersAvailable	Kafka not started or wrong port	Ensure Kafka container is running and port 9092 is open
UnknownTopicOrPartitionError	Topic not created	Create topics manually inside Kafka container
No alerts shown	Random data doesn’t include person_detected	Run test_send_event.py
CSV file missing	Ran consumer_storage.py from wrong folder	Run from project root

Author
Kuldeep Vighane
Cloud Operations Engineer
📍 Aurangabad, Maharashtra
📧 nishupvighane@gmail.com
